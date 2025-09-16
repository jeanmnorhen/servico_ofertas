import os
import json
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, auth, firestore
from confluent_kafka import Producer

app = Flask(__name__)
CORS(app)

# --- Inicialização do Firebase Admin SDK ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
cred_path = os.path.join(project_root, 'firebase-adminsdk.json')

db = None
if not firebase_admin._apps:
    try:
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
    except Exception as e:
        print(f"Erro ao inicializar o Firebase Admin SDK: {e}")

# --- Configuração do Kafka Producer ---
producer = None
try:
    kafka_conf = {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ.get('KAFKA_API_KEY'),
        'sasl.password': os.environ.get('KAFKA_API_SECRET')
    }
    if kafka_conf['bootstrap.servers']:
        producer = Producer(kafka_conf)
        print("Produtor Kafka inicializado com sucesso.")
    else:
        print("Variáveis de ambiente do Kafka não encontradas.")
except Exception as e:
    print(f"Erro ao inicializar Produtor Kafka: {e}")

def delivery_report(err, msg):
    if err is not None:
        print(f'Falha ao entregar mensagem Kafka: {err}')
    else:
        print(f'Mensagem Kafka entregue em {msg.topic()} [{msg.partition()}]')

def publish_event(topic, event_type, offer_id, data, changes=None):
    if not producer:
        print("Produtor Kafka não está inicializado. Evento não publicado.")
        return
    event = {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "offer_id": offer_id,
        "data": data,
        "source_service": "servico-ofertas"
    }
    if changes:
        event["changes"] = changes
    try:
        event_value = json.dumps(event, default=str)
        producer.produce(topic, key=offer_id, value=event_value, callback=delivery_report)
        producer.poll(0)
        print(f"Evento '{event_type}' para a oferta {offer_id} publicado no tópico {topic}.")
    except Exception as e:
        print(f"Erro ao publicar evento Kafka: {e}")

@app.route("/api/offers", methods=["POST"])
def create_offer():
    """Cria uma nova oferta, verificando se o usuário é dono do produto."""
    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    # 2. Validação dos Dados de Entrada
    offer_data = request.get_json()
    if not offer_data or not offer_data.get('product_id') or not offer_data.get('offer_price'):
        return jsonify({"error": "Product ID and offer price are required"}), 400
    
    product_id = offer_data['product_id']

    # 3. Autorização: Verifica se o usuário autenticado é o dono do produto
    try:
        product_ref = db.collection('products').document(product_id)
        product_doc = product_ref.get()
        if not product_doc.exists:
            return jsonify({"error": "Product not found"}), 404
        
        if product_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to create offers for this product"}), 403
        
        # Adiciona o store_id do produto na oferta para referência futura
        offer_data['store_id'] = product_doc.to_dict().get('store_id')

    except Exception as e:
        return jsonify({"error": "Could not verify product ownership", "details": str(e)}), 500

    # 4. Lógica de Negócio: Cria a oferta
    try:
        offer_to_create = offer_data.copy()
        offer_to_create['owner_uid'] = uid
        offer_to_create['created_at'] = firestore.SERVER_TIMESTAMP
        offer_to_create['updated_at'] = firestore.SERVER_TIMESTAMP
        _, doc_ref = db.collection('offers').add(offer_to_create)
        
        publish_event('eventos_ofertas', 'OfferCreated', doc_ref.id, offer_to_create)
        return jsonify({"message": "Offer created successfully", "offerId": doc_ref.id}), 201
    except Exception as e:
        return jsonify({"error": "Could not create offer", "details": str(e)}), 500

@app.route('/api/offers/<offer_id>', methods=['GET'])
def get_offer(offer_id):
    """Retorna os dados de uma oferta."""
    if not db:
        return jsonify({"error": "Dependências de banco de dados não inicializadas."}), 503

    try:
        offer_doc = db.collection('offers').document(offer_id).get()
        if not offer_doc.exists:
            return jsonify({"error": "Oferta não encontrada."}), 404
        offer_data = offer_doc.to_dict()
        offer_data['id'] = offer_doc.id
        return jsonify(offer_data), 200
    except Exception as e:
        return jsonify({"error": f"Erro ao buscar oferta: {e}"}), 500

@app.route('/api/offers/<offer_id>', methods=['PUT'])
def update_offer(offer_id):
    """Atualiza os dados de uma oferta, verificando se o usuário é dono."""
    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    update_data = request.get_json()
    if not update_data:
        return jsonify({"error": "Dados para atualização são obrigatórios."}), 400

    offer_ref = db.collection('offers').document(offer_id)
    
    try:
        offer_doc = offer_ref.get()
        if not offer_doc.exists:
            return jsonify({"error": "Oferta não encontrada."}), 404
        
        # 2. Autorização: Verifica se o usuário autenticado é o dono da oferta
        if offer_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to update this offer"}), 403

        update_data['updated_at'] = firestore.SERVER_TIMESTAMP
        offer_ref.update(update_data)

        publish_event('eventos_ofertas', 'OfferUpdated', offer_id, update_data)
        return jsonify({"message": "Oferta atualizada com sucesso.", "offerId": offer_id}), 200

    except Exception as e:
        return jsonify({"error": f"Erro ao atualizar oferta: {e}"}), 500

@app.route('/api/offers/<offer_id>', methods=['DELETE'])
def delete_offer(offer_id):
    """Deleta uma oferta, verificando se o usuário é dono."""
    # 1. Autenticação
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    id_token = auth_header.split('Bearer ')[1]
    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    offer_ref = db.collection('offers').document(offer_id)

    try:
        offer_doc = offer_ref.get()
        if not offer_doc.exists:
            return jsonify({"error": "Oferta não encontrada."}), 404
        
        # 2. Autorização: Verifica se o usuário autenticado é o dono da oferta
        if offer_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to delete this offer"}), 403

        offer_ref.delete()

        publish_event('eventos_ofertas', 'OfferDeleted', offer_id, {"offer_id": offer_id})
        return '', 204

    except Exception as e:
        return jsonify({"error": f"Erro ao deletar oferta: {e}"}), 500

# --- Health Check (para Vercel) ---
@app.route('/api/health', methods=['GET'])
def health_check():
    status = {
        "firestore": "ok" if db else "error",
        "kafka_producer": "ok" if producer else "error"
    }
    http_status = 200 if all(s == "ok" for s in status.values()) else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)
p.run(debug=True)
