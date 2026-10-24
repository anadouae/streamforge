# Méthodologie — StreamForge

1. **Cadrage** — plateforme d’événements Kafka réutilisable pour Urban Pulse.  
2. **Contrat** — `schemas/traffic_event_v1.json`.  
3. **Réalisation** — producer validant + consumer qualité.  
4. **Vérification** — `pytest` (validation) + smoke Kafka local.  
5. **Livraison** — Dockerfile + GitHub Actions CI.
