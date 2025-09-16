import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from firebase_admin import firestore

@pytest.fixture
def client():
    from api.index import app
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture(autouse=True)
def mock_all_dependencies():
    # 1. Mock Firebase
    mock_fs_doc = MagicMock()
    mock_fs_doc.exists = True
    mock_fs_doc.id = "test_offer_id"
    mock_fs_doc.to_dict.return_value = {
        'product_id': 'test_product_id',
        'offer_price': 79.99,
        'store_id': 'test_store_id',
        'owner_uid': 'test_owner_uid',
        'created_at': datetime.now(timezone.utc),
        'updated_at': datetime.now(timezone.utc)
    }

    # 2. Mock Kafka Producer
    mock_kafka_producer_instance = MagicMock()

    # Apply all mocks using patch
    with patch('api.index.db', MagicMock()) as mock_db, \
         patch('api.index.auth') as mock_auth, \
         patch('api.index.producer', mock_kafka_producer_instance), \
         patch('api.index.publish_event') as mock_publish_event:

        # Configure the mock for Firestore document retrieval
        mock_db.collection.return_value.document.return_value.get.return_value = mock_fs_doc
        
        yield {
            "db": mock_db,
            "auth": mock_auth,
            "producer": mock_kafka_producer_instance,
            "publish_event": mock_publish_event
        }

def test_create_offer_success(client, mock_all_dependencies):
    """Testa a criação de uma oferta por um usuário que é dono do produto."""
    # 1. Setup do Mock
    fake_token = "fake_token_for_offer_creation"
    headers = {"Authorization": f"Bearer {fake_token}"}
    user_uid = "test_owner_uid"
    product_id = "my_product_id_123"
    store_id = "my_store_id_456"

    # Mock da autenticação
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    # Mock da verificação de dono do produto (leitura no Firestore)
    mock_product_doc = MagicMock()
    mock_product_doc.exists = True
    mock_product_doc.to_dict.return_value = {'owner_uid': user_uid, 'name': 'Produto Teste', 'store_id': store_id}
    
    # Mock da criação da oferta (escrita no Firestore)
    mock_offer_doc_ref = MagicMock()
    mock_offer_doc_ref.id = "new_offer_id_789"
    
    # Configura o mock do cliente Firestore
    mock_all_dependencies["db"].collection.return_value.document.return_value.get.return_value = mock_product_doc
    mock_all_dependencies["db"].collection.return_value.add.return_value = (MagicMock(), mock_offer_doc_ref)

    # 2. Dados da Requisição
    new_offer_data = {
        "product_id": product_id,
        "offer_price": 79.99,
        "start_date": "2025-10-01T00:00:00Z",
        "end_date": "2025-10-10T23:59:59Z",
        "offer_type": "promocao"
    }

    # 3. Execução
    response = client.post("/api/offers", headers=headers, json=new_offer_data)

    # 4. Asserções
    assert response.status_code == 201
    assert response.json == {"message": "Offer created successfully", "offerId": "new_offer_id_789"}

    # Verifica a chamada de verificação de dono
    mock_all_dependencies["db"].collection.assert_any_call('products')
    mock_all_dependencies["db"].collection('products').document.assert_called_once_with(product_id)

    # Verifica a chamada de criação de oferta
    mock_all_dependencies["db"].collection.assert_any_call('offers')
    # Get the arguments passed to the add method
    args, kwargs = mock_all_dependencies["db"].collection('offers').add.call_args
    actual_offer_data = args[0]

    # Assert on the content of the dictionary, ignoring the timestamp objects
    assert actual_offer_data['product_id'] == new_offer_data['product_id']
    assert actual_offer_data['offer_price'] == new_offer_data['offer_price']
    assert actual_offer_data['start_date'] == new_offer_data['start_date']
    assert actual_offer_data['end_date'] == new_offer_data['end_date']
    assert actual_offer_data['offer_type'] == new_offer_data['offer_type']
    assert actual_offer_data['owner_uid'] == user_uid
    assert actual_offer_data['store_id'] == store_id
    assert isinstance(actual_offer_data['created_at'], type(firestore.SERVER_TIMESTAMP))
    assert isinstance(actual_offer_data['updated_at'], type(firestore.SERVER_TIMESTAMP))

    # Verifica que o evento Kafka foi publicado
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'OfferCreated'
    assert args[2] == "new_offer_id_789"

def test_get_offer_success(client, mock_all_dependencies):
    """Testa a recuperação de uma oferta existente."""
    response = client.get('/api/offers/test_offer_id')

    assert response.status_code == 200
    assert response.json['id'] == 'test_offer_id'
    assert response.json['product_id'] == 'test_product_id'

def test_get_offer_not_found(client, mock_all_dependencies):
    """Testa a recuperação de uma oferta inexistente."""
    mock_all_dependencies["db"].collection.return_value.document.return_value.get.return_value.exists = False
    response = client.get('/api/offers/non_existent_offer')
    assert response.status_code == 404

def test_update_offer_success(client, mock_all_dependencies):
    """Testa a atualização de uma oferta por um usuário autorizado."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_offer_update"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    update_data = {"offer_price": 69.99}
    response = client.put('/api/offers/test_offer_id', headers=headers, json=update_data)

    assert response.status_code == 200
    assert response.json['message'] == 'Oferta atualizada com sucesso.'
    assert response.json['offerId'] == 'test_offer_id'

    mock_all_dependencies["db"].collection.return_value.document.return_value.update.assert_called_once()
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'OfferUpdated'
    assert args[2] == 'test_offer_id'
    assert args[3]['offer_price'] == 69.99

def test_update_offer_unauthorized(client, mock_all_dependencies):
    """Testa a atualização de uma oferta por um usuário não autorizado."""
    unauthorized_uid = "unauthorized_user_uid"
    fake_token = "fake_token_for_unauthorized_update"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': unauthorized_uid}

    update_data = {"offer_price": 69.99}
    response = client.put('/api/offers/test_offer_id', headers=headers, json=update_data)

    assert response.status_code == 403
    assert response.json['error'] == 'User is not authorized to update this offer'

def test_delete_offer_success(client, mock_all_dependencies):
    """Testa a exclusão de uma oferta por um usuário autorizado."""
    user_uid = "test_owner_uid"
    fake_token = "fake_token_for_offer_delete"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': user_uid}

    response = client.delete('/api/offers/test_offer_id', headers=headers)

    assert response.status_code == 204
    mock_all_dependencies["db"].collection.return_value.document.return_value.delete.assert_called_once()
    mock_all_dependencies["publish_event"].assert_called_once()
    args, kwargs = mock_all_dependencies["publish_event"].call_args
    assert args[1] == 'OfferDeleted'
    assert args[2] == 'test_offer_id'

def test_delete_offer_unauthorized(client, mock_all_dependencies):
    """Testa a exclusão de uma oferta por um usuário não autorizado."""
    unauthorized_uid = "unauthorized_user_uid"
    fake_token = "fake_token_for_unauthorized_delete"
    headers = {"Authorization": f"Bearer {fake_token}"}
    
    mock_all_dependencies["auth"].verify_id_token.return_value = {'uid': unauthorized_uid}

    response = client.delete('/api/offers/test_offer_id', headers=headers)

    assert response.status_code == 403
    assert response.json['error'] == 'User is not authorized to delete this offer'

def test_health_check_all_ok(client, mock_all_dependencies):
    """Test health check when all services are up."""
    response = client.get('/api/health')
    assert response.status_code == 200
    assert response.json == {
        "firestore": "ok",
        "kafka_producer": "ok"
    }

def test_health_check_kafka_error(client, mock_all_dependencies):
    """Test health check when Kafka producer is not initialized."""
    with patch('api.index.producer', new=None):
        response = client.get('/api/health')
        assert response.status_code == 503
        assert response.json["kafka_producer"] == "error"
