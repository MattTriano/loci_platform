-- tests/assert_no_labeled_nonmatch_is_auto_merged.sql
-- The hand-labeled pairs are a regression fixture: any pair a human
-- judged NOT to be the same business must never land in the auto tier.
-- Returns rows (fails) if a rule change reintroduces a known false merge.
-- Pairs labeled null (undecidable) are deliberately not asserted on.

select d.name_a, d.name_b, d.decision
from {{ ref('chicago_contractor_pair_decisions') }} d
join {{ source('raw_data', 'labeled_chicago_contractor_pairs') }} l
  on l.name_a = d.name_a
 and l.name_b = d.name_b
where l.same_entity = false
  and d.decision = 'auto'
