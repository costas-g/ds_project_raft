from queue import Queue
import threading

# Παγκόσμιος χάρτης: node_id -> inbox Queue
inboxes = {}
lock = threading.Lock()

def register_node(node_id, inbox):
    """Κάθε κόμβος καταχωρεί την ουρά του στο transport layer."""
    with lock:
        inboxes[node_id] = inbox


def send_message(receiver_id, message):
    """Αποστολή μηνύματος στον κόμβο μέσω της ουράς του."""
    with lock:
        if receiver_id in inboxes:
            inboxes[receiver_id].put(message)
        else:
            print(f"[Transport] Warning: Node {receiver_id} is not registered.")


def reset_transport():
    """Καθαρίζει όλα τα inboxes (χρήσιμο για testing)."""
    with lock:
        inboxes.clear()
