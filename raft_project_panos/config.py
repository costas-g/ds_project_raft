# Λίστα με τα IDs των κόμβων , μπορουμε να βωαλουμε 5 η οσα θελουμε
NODE_IDS = [1, 2, 3]

# Εύρος χρονικού ορίου για εκλογές (σε δευτερόλεπτα)
ELECTION_TIMEOUT_RANGE = (1.5, 3.0)

# Διάστημα μεταξύ heartbeats για τον leader
HEARTBEAT_INTERVAL = 0.5

# Προαιρετικά: στατική χαρτογράφηση κόμβων σε θύρες ή endpoints
NODE_ENDPOINTS = {
    1: "localhost:5001",
    2: "localhost:5002",
    3: "localhost:5003"
}

# Μέγιστος αριθμός retries πριν θεωρηθεί ότι ένας κόμβος έχει αποτύχει
MAX_RETRIES = 3
