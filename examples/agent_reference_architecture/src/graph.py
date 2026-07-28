WORKERS = ["research_worker", "risk_worker"]

EDGES = [
    ("seed_request", "plan_request"),
    ("plan_request", WORKERS),
    (WORKERS, "assemble_candidate"),
    ("assemble_candidate", "review_candidate"),
    ("review_candidate", "revise_candidate"),
    ("revise_candidate", "review_candidate"),
    ("review_candidate", "publish_candidate"),
]
