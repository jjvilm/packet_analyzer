from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import atexit
from faker import Faker
import random
import time

from utils.utils import ones_complement_checksum


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PacketProducer:    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            enable_idempotence=True,
            retries=5,
            retry_backoff_ms=100,
            linger_ms=10,
            compression_type='lz4'
        )
        
        # Ensure cleanup on exit
        atexit.register(self.close)
    
    def send_packet(self, packet):
        """Send an order event to Kafka."""
        key = f"packet-{packet['packet_id']}"
        
        future = self.producer.send(
            topic='packets',
            key=key,
            value=packet
        )
        
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)
        
        return future
    
    def send_orders_batch(self, packets):
        """Send multiple orders and return any failures."""
        failures = []
        
        for packet in packets:
            future = self.send_packet(packet)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                failures.append({'packet': packet, 'error': str(e)})
        
        return failures
    
    def _on_success(self, metadata):
        logger.info(
            f"Message sent: topic={metadata.topic}, "
            f"partition={metadata.partition}, offset={metadata.offset}"
        )
    
    def _on_error(self, exception):
        logger.error(f"Message failed: {exception}")
    
    def close(self):
        """Flush and close the producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")


def generate_packet(packet_id, faker_instance, blacklist=None):
    protocol = random.choices(['TCP', 'UDP', 'ICMP'], weights=[0.7, 0.25, 0.05])[0]
    version = random.choices([4, 6], weights=[0.9, 0.1])[0]
    data_length = random.choices([16, 32, 64, 100, 128], weights=[0.2, 0.4, 0.2, 0.15, 0.05])[0]

 
    def pick_ip(ip_version):
        use_blacklist = blacklist and random.random() < 0.05  # 5% chance
        if use_blacklist:
            return random.choice(blacklist)
        return faker_instance.ipv4_public() if ip_version == 4 else faker_instance.ipv6()

    src_ip = pick_ip(version)
    dst_ip = pick_ip(version)

    # Compose header fields for checksum (excluding checksum itself)
    header_fields = {
        "packet_id": str(packet_id),
        "version": version,
        "data_length": data_length,
        "protocol": protocol,
        "src_ip": src_ip,
        "dst_ip": dst_ip
    }
    # Serialize header for checksum
    header_str = json.dumps(header_fields, sort_keys=True)
    # Calculate checksum
    checksum = ones_complement_checksum(header_str)
    # Now include checksum in header for length
    header_fields_with_checksum = dict(header_fields)
    header_fields_with_checksum["checksum"] = checksum
    header_str_with_checksum = json.dumps(header_fields_with_checksum, sort_keys=True)
    ip_header_length = len(header_str_with_checksum)

    src_port = random.choices([
        80, 443, 22, 53, 8080, random.randint(1024, 65535)
    ], weights=[0.2, 0.4, 0.1, 0.1, 0.1, 0.1])[0]
    dest_port = random.choices([
        80, 443, 22, 53, 8080, random.randint(1024, 65535)
    ], weights=[0.2, 0.4, 0.05, 0.05, 0.1, 0.2])[0]
    control_flags = random.choices([
        'SYN', 'ACK', 'FIN', 'PSH', 'RST', 'URG', 'NONE'
    ], weights=[0.3, 0.3, 0.1, 0.1, 0.05, 0.05, 0.1])[0] if protocol == 'TCP' else 'NONE'
    window_size = random.choices([0, 1024, 4096, 8192, 65535], weights=[0.1, 0.2, 0.3, 0.2, 0.2])[0]
    data = faker_instance.text(max_nb_chars=data_length) if data_length > 0 else ''
    timestamp = int(time.time())
    return {
        "packet_id": str(packet_id),
        "version": version,
        "ip_header_length": ip_header_length,
        "data_length": data_length,
        "protocol": protocol,
        "checksum": checksum,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "src_port": src_port,
        "dest_port": dest_port,
        "control_flags": control_flags,
        "window_size": window_size,
        "data": data,
        "timestamp": timestamp
    }

def produce_packets(bootstrap_servers = 'kafka:9092', blacklist_path = 'data/blacklist.txt'):
    producer = PacketProducer(bootstrap_servers)
    # Load blacklist.txt
    try:  
        with open(blacklist_path, 'r') as f:
            blacklist = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        blacklist = None
        print(f"Blacklist '{blacklist_path}' could not be found. Potentially malicious packets will not dropped.")


    faker_instance = Faker()
    produce = 500
    packet_id_start = 10000
    for i in range(produce):
        packet = generate_packet(packet_id_start + i, faker_instance, blacklist)
        print(f"Packet:\n{packet}")
        producer.send_packet(packet)
        time.sleep(random.uniform(0.05, 0.2))  # Simulate packet intervals


produce_packets()