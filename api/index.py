import os
import json
import base64
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_cors import CORS

try:
    import firebase_admin
    from firebase_admin import credentials, auth, firestore
except ImportError:
    firebase_admin = None

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None

# --- Variáveis globais para erros de inicialização ---
firebase_init_error = None
kafka_producer_init_error = None # Renamed for clarity

# --- Configuração do Flask ---
app = Flask(__name__)
CORS(app)

# --- Inicialização do Firebase Admin SDK (PADRONIZADO) ---
db = None
if firebase_admin:
    try:
        base64_sdk = os.environ.get('FIREBASE_ADMIN_SDK_BASE64')
        if base64_sdk:
            decoded_sdk = base64.b64decode(base64_sdk).decode('utf-8')
            cred_dict = json.loads(decoded_sdk)
            cred = credentials.Certificate(cred_dict)
            if not firebase_admin._apps:
                firebase_admin.initialize_app(cred)
            db = firestore.client()
            print("Firebase inicializado com sucesso via Base64.")
        else:
            firebase_init_error = "Variável de ambiente FIREBASE_ADMIN_SDK_BASE64 não encontrada."
            print(firebase_init_error)
    except Exception as e:
        firebase_init_error = str(e)
        print(f"Erro ao inicializar o Firebase Admin SDK: {e}")
else:
    firebase_init_error = "Biblioteca firebase_admin não encontrada."

# --- Configuração do Kafka Producer ---
producer = None
if Producer:
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
            kafka_producer_init_error = "Variáveis de ambiente do Kafka não encontradas."
            print(kafka_producer_init_error)
    except Exception as e:
        kafka_producer_init_error = str(e)
        print(f"Erro ao inicializar Produtor Kafka: {e}")
else:
    kafka_producer_init_error = "Biblioteca confluent_kafka não encontrada."

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
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    offer_data = request.get_json()
    if not offer_data or not offer_data.get('product_id') or not offer_data.get('offer_price'):
        return jsonify({"error": "Product ID and offer price are required"}), 400
    
    product_id = offer_data['product_id']

    try:
        product_ref = db.collection('products').document(product_id)
        product_doc = product_ref.get()
        if not product_doc.exists:
            return jsonify({"error": "Product not found"}), 404
        
        if product_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to create offers for this product"}), 403
        
        offer_data['store_id'] = product_doc.to_dict().get('store_id')

    except Exception as e:
        return jsonify({"error": "Could not verify product ownership", "details": str(e)}), 500

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
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

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
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
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
    if not db:
        return jsonify({"error": "Dependência do Firestore não inicializada."}), 503

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "Authorization token is required"}), 401

    try:
        id_token = auth_header.split('Bearer ')[1]
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token['uid']
    except Exception as e:
        return jsonify({"error": "Invalid or expired token", "details": str(e)}), 401

    offer_ref = db.collection('offers').document(offer_id)

    try:
        offer_doc = offer_ref.get()
        if not offer_doc.exists:
            return jsonify({"error": "Oferta não encontrada."}), 404
        
        if offer_doc.to_dict().get('owner_uid') != uid:
            return jsonify({"error": "User is not authorized to delete this offer"}), 403

        offer_ref.delete()

        publish_event('eventos_ofertas', 'OfferDeleted', offer_id, {"offer_id": offer_id})
        return '', 204

    except Exception as e:
        return jsonify({"error": f"Erro ao deletar oferta: {e}"}), 500

def get_health_status():
    env_vars = {
        "FIREBASE_ADMIN_SDK_BASE64": "present" if os.environ.get('FIREBASE_ADMIN_SDK_BASE64') else "missing",
        "KAFKA_BOOTSTRAP_SERVER": "present" if os.environ.get('KAFKA_BOOTSTRAP_SERVER') else "missing",
        "KAFKA_API_KEY": "present" if os.environ.get('KAFKA_API_KEY') else "missing",
        "KAFKA_API_SECRET": "present" if os.environ.get('KAFKA_API_SECRET') else "missing"
    }
    status = {
        "environment_variables": env_vars,
        "dependencies": {
            "firestore": "ok" if db else "error",
            "kafka_producer": "ok" if producer else "error"
        },
        "initialization_errors": {
            "firestore": firebase_init_error,
            "kafka_producer": kafka_producer_init_error
        }
    }
    return status

@app.route('/api/health', methods=['GET'])
def health_check():
    status = get_health_status()
    
    all_ok = (
        all(value == "present" for value in status["environment_variables"].values()) and
        status["dependencies"]["firestore"] == "ok" and
        status["dependencies"]["kafka_producer"] == "ok"
    )
    http_status = 200 if all_ok else 503
    return jsonify(status), http_status

if __name__ == '__main__':
    app.run(debug=True)