from producer_new import KafkaTweetProducer

# Test koneksi
try:
    producer = KafkaTweetProducer(topic="Twitter-raw")
    
    # Kirim test message
    test_data = {
        "keyword": "test",
        "text": "Test message",
        "timestamp": "2025-11-30"
    }
    
    producer.send(test_data)
    print("\n✓ Test berhasil!")
    
except Exception as e:
    print(f"\n✗ Test gagal: {e}")
    import traceback
    traceback.print_exc()
finally:
    producer.close()