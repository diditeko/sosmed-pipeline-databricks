import json
import yaml
import certifi
from kafka import KafkaProducer
from decouple import config
from pathlib import Path

CURRENT_FILE_DIR = Path(__file__).resolve().parent

def load_kafka_config(config_file_name="kafka_configconfluent.yaml"):
    """Memuat konfigurasi Kafka dari file YAML dan resolve environment variables"""
    absolute_config_path = CURRENT_FILE_DIR.parent.parent / "configs" / config_file_name
    print(f"DEBUG: Memuat config dari: {absolute_config_path}") 
    print(f"DEBUG: Apakah file ditemukan: {absolute_config_path.exists()}")
    
    if not absolute_config_path.exists():
        raise FileNotFoundError(f"File konfigurasi tidak ditemukan: {absolute_config_path}")
    
    with open(absolute_config_path, 'r') as f:
        kafka_config = yaml.safe_load(f)
    
    # Resolve environment variables in security section
    if 'security' in kafka_config:
        security = kafka_config['security']
        
        # Replace ${KAFKA_API_KEY} with actual value from .env
        if security.get('api_key', '').startswith('${'):
            env_var = security['api_key'].strip('${}')
            security['api_key'] = config(env_var)
            print(f"DEBUG: Resolved {env_var} from environment")
        
        # Replace ${KAFKA_API_SECRET} with actual value from .env
        if security.get('api_secret', '').startswith('${'):
            env_var = security['api_secret'].strip('${}')
            security['api_secret'] = config(env_var)
            print(f"DEBUG: Resolved {env_var} from environment")
    
    return kafka_config

# Memuat konfigurasi saat modul dimulai
KAFKA_CONFIG = load_kafka_config("kafka_configconfluent.yaml")

# PERBAIKAN: Gunakan config() bukan config.get()
KAFKA_BOOTSTRAP_SERVERS = config("KAFKA_BOOTSTRAP_SERVERS")

class KafkaTweetProducer:
    def __init__(self, topic: str, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, use_certifi=True):
        """
        Inisialisasi Kafka Producer untuk Confluent Cloud
        
        Args:
            topic: Nama topik Kafka
            bootstrap_servers: URL bootstrap servers
            use_certifi: Jika True, gunakan certifi untuk CA bundle (default: True)
        """
        
        # Ambil semua konfigurasi yang diperlukan
        producer_opts = KAFKA_CONFIG.get('producer', {})
        security_opts = KAFKA_CONFIG.get('security', {})
        
        final_bootstrap_servers = bootstrap_servers 
        self.topic = topic
        
        # SOLUSI SSL CERTIFICATE
        if use_certifi:
            # Opsi 1: Gunakan certifi (bundle CA certificates yang terpercaya)
            ca_bundle = certifi.where()
            print(f"✓ Menggunakan CA bundle dari certifi: {ca_bundle}")
        else:
            # Opsi 2: Gunakan custom CA bundle (jika ada)
            ca_bundle = CURRENT_FILE_DIR.parent.parent / "configs" / "cacert.pem"
            if not ca_bundle.exists():
                print(f"⚠ File {ca_bundle} tidak ditemukan, fallback ke certifi")
                ca_bundle = certifi.where()
            else:
                ca_bundle = str(ca_bundle)
                print(f"✓ Menggunakan CA bundle custom: {ca_bundle}")
        
        print(f"DEBUG: Connecting to Kafka at {final_bootstrap_servers}")

        # Inisialisasi KafkaProducer
        self.producer = KafkaProducer(
            # Konfigurasi wajib
            bootstrap_servers=final_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            
            # Opsi Producer dari YAML (acks, retries, dll.)
            **producer_opts,
            
            # Opsi Keamanan
            security_protocol=security_opts['protocol'],
            sasl_mechanism=security_opts['sasl_mechanism'],
            sasl_plain_username=security_opts["api_key"],
            sasl_plain_password=security_opts["api_secret"],
            
            # SSL Configuration - Gunakan CA bundle yang sudah ditentukan
            ssl_cafile=ca_bundle
        )
        
        print("✓ Kafka Producer berhasil diinisialisasi dan terkoneksi")


    def send(self, data: dict):
        """Mengirim data ke topik Kafka"""
        if not data:
            print("⚠ Peringatan: Data kosong, tidak mengirim pesan.")
            return

        # Gunakan 'keyword' sebagai key, atau 'default' jika tidak ada
        key = data.get("keyword", "default")
        
        try:
            future = self.producer.send(self.topic, key=key, value=data)
            self.producer.flush()
            
            # Konfirmasi pengiriman
            record_metadata = future.get(timeout=10) 
            print(f"✓ Pesan terkirim ke topik '{record_metadata.topic}', "
                  f"partisi {record_metadata.partition}, offset {record_metadata.offset}")
            
            return record_metadata

        except Exception as e:
            print(f"✗ ERROR: Gagal mengirim pesan ke Kafka: {e}")
            raise
            
    def close(self):
        """Menutup koneksi producer"""
        try:
            self.producer.close()
            print("✓ Kafka Producer ditutup")
        except Exception as e:
            print(f"⚠ Error saat menutup producer: {e}")


# # Contoh penggunaan
# if __name__ == "__main__":
#     # Contoh inisialisasi dan pengiriman pesan
#     producer = KafkaTweetProducer(topic="tweets-topic")
    
#     try:
#         # Kirim pesan contoh
#         sample_data = {
#             "keyword": "kafka",
#             "text": "Testing Confluent Cloud connection",
#             "timestamp": "2025-11-30T12:00:00"
#         }
        
#         producer.send(sample_data)
        
#     finally:
#         producer.close()