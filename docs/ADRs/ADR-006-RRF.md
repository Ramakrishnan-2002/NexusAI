# ADR-006: Rank-Based Fusion via Reciprocal Rank Fusion (RRF)

## Status
**ACCEPTED**

## Context
In hybrid search, vector similarity scores (cosine distance $[-1, 1]$) and lexical full-text scores (BM25 term frequencies $[0, \infty)$) follow fundamentally different mathematical distributions. Direct weighted score averaging ($\alpha \cdot S_{\text{vec}} + \beta \cdot S_{\text{fts}}$) is fragile, scale-dependent, and requires brittle domain-specific calibration.

## Decision
Use **Reciprocal Rank Fusion (RRF)** to combine ranked candidate lists:
$$\text{RRF}(d) = \sum_{m \in \{\text{vector}, \text{fts}\}} \frac{1}{k + \text{rank}_m(d)}$$
with standard dampening constant $k = 60$.

## Alternatives Considered
1. **Weighted Score Normalization (Min-Max):** Outliers skew the distribution, causing one search channel to dominate unpredictably.
2. **Cross-Encoder Reranking Only:** Computationally expensive ($>50\text{ms}$) to evaluate all raw database candidates with a neural cross-encoder.

## Consequences
- **Positive:** Scale-invariant, robust, and prioritizes documents appearing consistently near the top across both retrieval channels.
- **Tradeoff:** Rank positions discard magnitude differences between adjacent items within a single search list.
