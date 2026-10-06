"""
Aggregator Service
==================
Collects item results from orders.results, grouped by orderId, and publishes
exactly one completion message per order to orders.complete: "complete" when
every item has reported, or "partial" when the order has been idle too long.
"""

import json
import pika
import os
import threading
import time


# orderId -> {"total": int, "results": {itemIndex: result}, "last_seen": float}
# Results are keyed by itemIndex, so a redelivered duplicate overwrites
# nothing and is simply ignored: it can never inflate the count.
in_flight = {}

# orderId -> time it was closed. Without this, a late duplicate (or a result
# arriving after a partial was sent) would open a brand-new entry and
# eventually produce a SECOND completion message for the same order.
closed = {}
CLOSED_TTL_SECONDS = 600.0

lock = threading.Lock()

IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))
SWEEP_INTERVAL_SECONDS = 1.0


def log(text):
    print(text, flush=True)


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    """Publish a single message to orders.complete. Called with the lock
    already released -- do not hold `lock` while doing network I/O."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent
    )
    connection.close()


def build_message(order_id, state, status):
    """Build the orders.complete message from an order's state."""
    results = state['results']
    missing = sorted(set(range(state['total'])) - set(results))
    return {
        'orderId': order_id,
        'correlationId': order_id,
        'status': status,
        'totalItems': state['total'],
        'receivedItems': len(results),
        'itemResults': [results[i] for i in sorted(results)],
        'missingItemIndexes': missing,
    }


def safe_publish(message):
    """Publish, but never let a broker hiccup kill the calling thread."""
    try:
        publish_completion(message)
        log(f"[Aggregator] Order {message['orderId']} -> {message['status']} "
            f"({message['receivedItems']}/{message['totalItems']})")
    except Exception as exc:
        log(f"[Aggregator] ERROR publishing completion for "
            f"{message['orderId']}: {exc!r}")


def aggregate_result(ch, method, properties, body):
    """Handle one message from orders.results."""
    try:
        result = json.loads(body)
        order_id = result['orderId']
        index = int(result['itemIndex'])
        total = int(result['totalItems'])
    except (ValueError, KeyError, TypeError) as exc:
        # A message we cannot interpret must not jam the queue: log and ack.
        log(f"[Aggregator] Dropping unparseable result ({exc!r}): {body!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    to_publish = None

    with lock:
        if order_id in closed:
            log(f"[Aggregator] Ignoring result for already-closed order "
                f"{order_id} item {index}")
        else:
            state = in_flight.get(order_id)
            if state is None:
                state = {'total': total, 'results': {}, 'last_seen': time.monotonic()}
                in_flight[order_id] = state

            if not 0 <= index < state['total']:
                log(f"[Aggregator] Ignoring out-of-range itemIndex {index} "
                    f"for order {order_id}")
            elif index in state['results']:
                log(f"[Aggregator] Ignoring duplicate result for order "
                    f"{order_id} item {index}")
            else:
                state['results'][index] = result
                state['last_seen'] = time.monotonic()

                if len(state['results']) == state['total']:
                    to_publish = build_message(order_id, state, 'complete')
                    del in_flight[order_id]
                    closed[order_id] = time.monotonic()

    # Publish AFTER releasing the lock.
    if to_publish is not None:
        safe_publish(to_publish)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """Background thread: turn silent, incomplete orders into partial results."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        try:
            now = time.monotonic()
            to_publish = []

            with lock:
                expired = [oid for oid, st in in_flight.items()
                           if now - st['last_seen'] > IDLE_TIMEOUT_SECONDS]
                for oid in expired:
                    to_publish.append(build_message(oid, in_flight[oid], 'partial'))
                    del in_flight[oid]
                    closed[oid] = now

                # Forget old tombstones so memory doesn't grow forever.
                for oid in [o for o, t in closed.items()
                            if now - t > CLOSED_TTL_SECONDS]:
                    del closed[oid]

            for message in to_publish:
                safe_publish(message)
        except Exception as exc:
            log(f"[Aggregator] Sweep error: {exc!r}")


def main():
    """Main entry point: connect to RabbitMQ, start the timeout sweeper,
    and start consuming results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    channel.basic_qos(prefetch_count=1)

    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    log('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()