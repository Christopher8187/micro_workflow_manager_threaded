# Geometry solver lab

A renamed subset of a parser/autofix/solver/validator/formatter architecture.
Validation is deliberately independent from solving, and the final output
retains the seed, coordinate-system choice, tolerance, and formatting decision.

```bash
mwf init
mwf graph src/graph.py
mwf runfrom parse_construction
mwf inspect solve_coordinates filter
mwf inspect validate_solution job 1
```
