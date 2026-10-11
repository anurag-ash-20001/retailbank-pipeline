# Data-quality / rejects report

Generated from the live pipeline tables after the end-to-end run (reset → Day 1 → Day 2) on Databricks.
Sources: `rb_gold.kpi10_dq_scorecard`, `rb_silver.quarantine`, `rb_audit.dq_check_results`, `rb_audit.dq_rule_catalog`, `rb_audit.data_profile`.

## 1. What profiling found (Day 1 raw files)

| Entity | Issues found in the raw data | Handling |
|---|---|---|
| **branches.csv** (102 rows / 99 ids) | 3 re-sent branches (B002, B012, B051) with trailing spaces & a different phone format; 4 blank managers; region in 8 spellings (`NORTH`, `south `, ` West `…); branch_type casing (`RURAL`, `urban`); 4 non-ISO opened dates; phones as `1200471138`, `+91 70985 93174`, 8-digit `+91-48175496` | dedupe keeps the cleanest copy; region / type / date / phone standardized; missing manager & invalid phone flagged (NULL) |
| **customers.csv** (257 rows / 251 ids) | 6 exact re-sends differing only in name whitespace / e-mail case; 4 customers without account_id; A0046 claimed by C046 and C8252; e-mails with `@@`, missing `@`, blanks, upper case; phones in 6 formats incl. 8-digit; gender in 9 spellings; KYC in 8 spellings; DOB blank / in 2028 / in 1901; registration_date in 4 formats; ~85 "variant" customer ids that are the same person (same name + DOB) with their own account | exact dups dropped (logged); missing / shared account quarantined; e-mail repaired or NULLed; phone → `+91-XXXXXXXXXX` or NULL; gender / KYC mapped; invalid DOB NULLed + flagged; dates parsed; same-person variants linked via `potential_duplicate_of` (not merged – they own separate accounts with transactions) |
| **products_json.txt** (114 objects / 111 ids) | **JSON structural defect: missing comma between objects at line 41** (concatenated feed); P002 / P063 / P089 duplicated later in the feed with different prices; 3 negative prices (P298-P300); `is_active` as boolean, string (`"true"`, `"FALSE"`), null and missing; category casing / trailing space; blank product name | document salvaged object-by-object, defect logged in `ingestion_log.structural_issues`; later copy wins for re-sent products; negative prices quarantined; `is_active` → boolean (missing → FALSE, flagged); category standardized |
| **transactions.csv** (940 rows / 900 ids) | 22 exact duplicate rows; 18 ids re-used with a different amount/status; amounts blank (22), negative (16), zero (14), `Rs.`-prefixed (9), comma-formatted (11); status typos (`Succes`, `sucess`, `Faild`, casing); payment-method casing / trailing space; currency `inr`, EUR, USD; `is_refund` as True/False/1/0/Y/N/blank; **`date` in 4 formats and disagreeing with `timestamp` in ~200 rows**; orphan accounts (A9xxx), branches (B9xx), products (P9xx, P298-P300) | see rule catalogue: exact dups dropped, conflicting ids quarantined (all versions), bad amounts quarantined, typos mapped, non-INR flagged for FX review, timestamp is authoritative, orphans quarantined and **retried each run** |

Day 2 files add: new branch B901 + manager change on B069; 22 new customers + 25 updates (9 KYC Pending→Verified, 9 address changes, phone changes, blank e-mails); 9 product price/status changes + 8 new products + a corrected **P299** (was negative on Day 1); 199 transactions including **12 late corrections to Day 1 ids** (`[late correction]` remarks), new conflicting duplicates and new orphans.

## 2. Results
### KPI 10 – scorecard per file per run

| batch | source_file | received | passed | rejected | dup_removed | invalid_rejected | fixed_and_passed | pass_rate_pct | top3_rejection_reasons | recovered |
|---|---|---|---|---|---|---|---|---|---|---|
| day1 | branches.csv | 102 | 99 | 3 | 3 | 0 | 20 | 97.06 | DUPLICATE_SUPERSEDED (3) |  |
| day1 | customers.csv | 257 | 246 | 11 | 6 | 5 | 136 | 95.72 | DUPLICATE_EXACT (6), MISSING_ACCOUNT_ID (4), SHARED_ACCOUNT_ID (1) |  |
| day1 | products_json.txt | 114 | 108 | 6 | 3 | 3 | 14 | 94.74 | DUPLICATE_SUPERSEDED (3), NEGATIVE_PRICE (3) |  |
| day1 | transactions.csv | 940 | 722 | 218 | 21 | 197 | 497 | 76.81 | ORPHAN_PRODUCT (56), CONFLICTING_DUPLICATE_ID (32), ORPHAN_ACCOUNT (29) | 0 |
| day2 | branches_day2.csv | 2 | 2 | 0 | 0 | 0 | 0 | 100.0 |  |  |
| day2 | customer_updates_day2.csv | 47 | 47 | 0 | 0 | 0 | 18 | 100.0 |  |  |
| day2 | products_day2_json.txt | 18 | 18 | 0 | 0 | 0 | 1 | 100.0 |  |  |
| day2 | transactions_day2.csv | 199 | 148 | 51 | 4 | 47 | 92 | 74.37 | ORPHAN_PRODUCT (15), ORPHAN_BRANCH (9), CONFLICTING_DUPLICATE_ID (6) | 6 |

### Rejections by reason code

| batch | entity | reason_code | severity | records | meaning |
|---|---|---|---|---|---|
| day1 | branches | DUPLICATE_SUPERSEDED | INFO | 3 | Same natural key, different payload (dimension feeds: last-write-wins after completeness) |
| day1 | customers | DUPLICATE_EXACT | INFO | 6 | Re-sent record identical after trimming / case normalization |
| day1 | customers | MISSING_ACCOUNT_ID | ERROR | 4 | Customer has no linked account - cannot join transactions |
| day1 | customers | SHARED_ACCOUNT_ID | ERROR | 1 | Two customers claim one account_id. Owner = customer whose number matches the account number, else existing owner, else earliest registration |
| day1 | products | DUPLICATE_SUPERSEDED | INFO | 3 | Same natural key, different payload (dimension feeds: last-write-wins after completeness) |
| day1 | products | NEGATIVE_PRICE | ERROR | 3 | Price < 0 |
| day1 | transactions | ORPHAN_PRODUCT | ERROR | 56 | product_id not in product dimension |
| day1 | transactions | CONFLICTING_DUPLICATE_ID | ERROR | 32 | Same transaction_id, different payload within one batch - cannot tell which is true |
| day1 | transactions | ORPHAN_ACCOUNT | ERROR | 29 | account_id not in customer dimension |
| day1 | transactions | ORPHAN_BRANCH | ERROR | 28 | branch_id not in branch dimension |
| day1 | transactions | MISSING_AMOUNT | ERROR | 22 | Amount blank |
| day1 | transactions | DUPLICATE_EXACT | INFO | 21 | Re-sent record identical after trimming / case normalization |
| day1 | transactions | NEGATIVE_AMOUNT | ERROR | 16 | Amount < 0 (refunds are signalled by is_refund, not sign) |
| day1 | transactions | ZERO_AMOUNT | ERROR | 14 | Amount = 0 |
| day2 | transactions | ORPHAN_PRODUCT | ERROR | 15 | product_id not in product dimension |
| day2 | transactions | ORPHAN_BRANCH | ERROR | 9 | branch_id not in branch dimension |
| day2 | transactions | ORPHAN_ACCOUNT | ERROR | 6 | account_id not in customer dimension |
| day2 | transactions | CONFLICTING_DUPLICATE_ID | ERROR | 6 | Same transaction_id, different payload within one batch - cannot tell which is true |
| day2 | transactions | ZERO_AMOUNT | ERROR | 5 | Amount = 0 |
| day2 | transactions | DUPLICATE_EXACT | INFO | 4 | Re-sent record identical after trimming / case normalization |
| day2 | transactions | NEGATIVE_AMOUNT | ERROR | 3 | Amount < 0 (refunds are signalled by is_refund, not sign) |
| day2 | transactions | MISSING_AMOUNT | ERROR | 3 | Amount blank |

### Fixes applied to records that passed (dq_flags, both batches)

| entity | flag | records |
|---|---|---|
| branches | REGION_STANDARDIZED | 5 |
| branches | MANAGER_MISSING | 4 |
| branches | BRANCH_TYPE_STANDARDIZED | 4 |
| branches | DATE_FORMAT_STANDARDIZED | 4 |
| branches | PHONE_STANDARDIZED | 2 |
| branches | PHONE_INVALID | 1 |
| customers | POTENTIAL_DUPLICATE_PERSON | 134 |
| customers | PHONE_STANDARDIZED | 13 |
| customers | EMAIL_REPAIRED | 10 |
| customers | GENDER_STANDARDIZED | 10 |
| customers | KYC_STANDARDIZED | 9 |
| customers | DATE_FORMAT_STANDARDIZED | 6 |
| customers | DOB_MISSING | 5 |
| customers | EMAIL_MISSING | 5 |
| customers | DOB_FUTURE | 4 |
| customers | PHONE_INVALID | 3 |
| customers | DOB_IMPLAUSIBLE | 2 |
| customers | DATE_AMBIGUOUS_DDMM | 2 |
| products | CATEGORY_STANDARDIZED | 5 |
| products | IS_ACTIVE_STANDARDIZED | 4 |
| products | IS_ACTIVE_DEFAULTED | 3 |
| products | PRODUCT_NAME_MISSING | 3 |
| transactions | DATE_FORMAT_STANDARDIZED | 308 |
| transactions | DATE_TIMESTAMP_MISMATCH | 238 |
| transactions | IS_REFUND_DEFAULTED | 125 |
| transactions | STATUS_STANDARDIZED | 104 |
| transactions | PAYMENT_METHOD_STANDARDIZED | 65 |
| transactions | NON_INR_REVIEW | 19 |
| transactions | AMOUNT_REFORMATTED | 17 |
| transactions | LATE_CORRECTION_HINT | 10 |
| transactions | SOURCE_DATE_FUTURE | 9 |
| transactions | RECOVERED_FROM_QUARANTINE | 6 |

### Automated DQ checks (non-PASS)

| batch_id | check_name | entity | severity | status | observed | expected |
|---|---|---|---|---|---|---|
| day1 | customer_email_completeness | dim_customer | WARN | FAIL | 3 | 0 |
| day1 | fact_non_inr_pending_review | fact_transaction | WARN | FAIL | 16 | 0 |
| day2 | customer_email_completeness | dim_customer | WARN | FAIL | 5 | 0 |
| day2 | fact_non_inr_pending_review | fact_transaction | WARN | FAIL | 19 | 0 |

### Automated DQ checks – totals

| batch_id | checks | passed | blocking_failures | warnings |
|---|---|---|---|---|
| day1 | 34 | 32 | 0 | 2 |
| day2 | 34 | 32 | 0 | 2 |

### Day 2 – load reconciliation (KPI 11)

| entity | records_received | records_rejected | new_records | updated_scd2_new_version | updated_in_place | corrected_late_arriving | unchanged | recovered_from_prior_quarantine |
|---|---|---|---|---|---|---|---|---|
| branches | 2 | 0 | 1 | 1 | 0 |  | 0 |  |
| customers | 47 | 0 | 22 | 14 | 2 |  | 9 |  |
| products | 18 | 0 | 9 | 9 | 0 |  | 0 |  |
| transactions | 199 | 51 | 138 |  |  | 10 | 0 | 6 |


### Day 2 – late-arriving corrections applied in place

| transaction_id | day1_status | corrected_status | amount | version | correction_count |
|---|---|---|---|---|---|
| T0053 | Pending | Success | 170344.00 | 2 | 1 |
| T0064 | Failed | Success | 14251.00 | 2 | 1 |
| T0097 | Success | Failed | 6329.00 | 2 | 1 |
| T0256 | Success | Failed | 145440.00 | 2 | 1 |
| T0358 | Pending | Success | 104243.00 | 2 | 1 |
| T0427 | Pending | Success | 51513.00 | 2 | 1 |
| T0577 | Failed | Success | 37293.00 | 2 | 1 |
| T0598 | Failed | Success | 192325.00 | 2 | 1 |
| T0793 | Success | Failed | 135068.00 | 2 | 1 |
| T0849 | Success | Failed | 70732.00 | 2 | 1 |

### Quarantine entries resolved on Day 2

| rejected_in | record_key | original_reasons | resolved_in | resolution |
|---|---|---|---|---|
| day1 | T0060 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0273 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0488 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0499 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0514 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0676 | ORPHAN_PRODUCT | day2 | RECOVERED_LATE_ARRIVING_DIMENSION |
| day1 | T0504 | ORPHAN_PRODUCT | day2 | SUPERSEDED_BY_NEWER_RECORD |
| day1 | T0824 | ORPHAN_ACCOUNT | day2 | SUPERSEDED_BY_NEWER_RECORD |


## 3. Notes

* `received = passed + rejected` is asserted per entity and batch (`reconciliation_received_eq_passed_plus_rejected`); the job fails otherwise.
* The two warnings are informational by design: customers without a usable e-mail, and non-INR transactions awaiting FX review.
* 2 of the 12 Day-2 corrections target Day-1 transactions that were themselves quarantined (orphan account/product) – the
  Day-2 version supersedes the old quarantine row and is validated on its own (still an orphan → quarantined again).
* 6 Day-1 transactions on product P299 were recovered on Day 2 once the corrected product arrived (late-arriving dimension).
