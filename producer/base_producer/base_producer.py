from __future__ import annotations
import orjson
import uuid
from datetime import datetime, timezone
from typing import Optional, Callable, Dict, Any
from confluent_kafka import Producer
from confluent_kafka import KafkaError

# Type alias for delivery callback signature used by confluent-kafka
DeliveryCallback = Callable[[Optional[KafkaError], dict], None]


def _default_value_serializer(payload: Dict[str, Any]) -> bytes:
    """
    Default serializer using orjson.
    - Automatically handles datetime (to RFC 3339 / ISO 8601).
    - Automatically handles UUID.
    - Returns bytes directly.
    """
    return orjson.dumps(payload)


def _default_key_serializer(key: Optional[Any]) -> Optional[bytes]:
    """Default key serializer: handle UUIDs and strings, convert to UTF-8 bytes."""
    if key is None:
        return None
    if isinstance(key, uuid.UUID):
        return str(key).encode("utf-8")
    return str(key).encode("utf-8")


class BaseProducer:
    """
    Improved base producer.

    Parameters
    ----------
    kafka_conf : Optional[dict]
        confluent_kafka.Producer configuration dict.
    value_serializer : Callable[[dict], bytes]
        Function that turns a payload dict into bytes (default: JSON).
    key_serializer : Callable[[Any], Optional[bytes]]
        Function that turns a key into bytes (default: handles str/UUID).
    """

    def __init__(
        self,
        kafka_conf: Optional[Dict[str, Any]] = None,
        value_serializer: Callable[[Dict[str, Any]], bytes] = _default_value_serializer,
        key_serializer: Callable[[Optional[Any]], Optional[bytes]] = _default_key_serializer,
    ):
        self._value_serializer = value_serializer
        self._key_serializer = key_serializer
        
        self._producer = None
        if kafka_conf:
            if Producer is None:
                raise RuntimeError("confluent_kafka is required to use kafka_conf but is not installed")
            self._producer = Producer(kafka_conf)

    # -------------------------
    # Default delivery callback
    # -------------------------
    def _default_delivery_callback(self, err: Optional[KafkaError], msg_info: dict) -> None:
        """
        Default delivery callback. 
        """
        if err is not None:
            print(f"[kafka][delivery-failed] topic={msg_info.get('topic')} key={msg_info.get('key')!s} err={err}")
        else:
            print(f"[kafka][delivered] topic={msg_info.get('topic')} partition={msg_info.get('partition')} offset={msg_info.get('offset')} key={msg_info.get('key')!s}")

    # -------------------------
    # Produce wrapper
    # -------------------------
    def produce(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[Any] = None,
        on_delivery: Optional[DeliveryCallback] = None,
        flush: bool = False,
    ) -> None:
        """
        Produce a message.
        """
        serialized_value = self._value_serializer(value)
        serialized_key = self._key_serializer(key)

        if self._producer is None:
            # No Kafka -> debug print
            k = serialized_key.decode("utf-8") if serialized_key is not None else None
            print(f"[{topic}] key={k!s} value={serialized_value.decode('utf-8')}")
            return

        # Prepare callback wrapper
        def _cb(err, msg):
            msg_info = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": serialized_key.decode("utf-8") if serialized_key is not None else None,
            }
            # Use the passed callback, or fall back to the instance method
            callback_to_use = on_delivery or self._default_delivery_callback
            callback_to_use(err, msg_info)
        
        print(f"[kafka][producing] topic={topic} key={serialized_key.decode('utf-8') if serialized_key else None} value={serialized_value.decode('utf-8')}")

        self._producer.produce(topic=topic, value=serialized_value, key=serialized_key, callback=_cb)

        if flush:
            self.flush()

    # -------------------------
    # Flush / close
    # -------------------------
    def flush(self, timeout: Optional[float] = None) -> None:
        if self._producer:
            self._producer.flush(timeout)