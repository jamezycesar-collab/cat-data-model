# Trade Lifecycle State Machines

---

## 1. `order_request.request_status`

```
RECEIVED -> SYSTEMATIZED -> ORDER_CREATED
RECEIVED -> REJECTED
RECEIVED -> EXPIRED (clock sync breach, no order created within window)
```

**Transition rules:**

- `RECEIVED` is the only allowed entry state (receipt from customer).
- `SYSTEMATIZED` requires non-null `systematized_timestamp`.
- `REJECTED` is terminal; requires a rejection reason.
- `EXPIRED` signals CAT clock-sync breach (> 50 ms for electronic, > 1 s for manual).

## 2. `order_event.order_status`

```
NEW -> ACCEPTED -> WORKING -> PARTIALLY_FILLED -> FILLED
 ↓ ↓
 CANCELLED CANCELLED
 ↓ ↓
 REJECTED BUST
NEW -> REJECTED
NEW -> EXPIRED
WORKING -> MODIFIED -> WORKING (loop; logs MEOM)
```

**Transition rules:**

- `NEW` only on MENO/MONO/MLNO/MECO ingest.
- `ACCEPTED` ↔ `WORKING` is idempotent for algos.
- `PARTIALLY_FILLED` -> `FILLED` only when cumulative fill qty = order qty.
- `BUST` reverses prior fills (logs corresponding execution.execution_type=BUST).
- `EXPIRED` applies to GTD/GTC orders past their end-time.

## 3. `order_route.route_status`

```
SENT -> ACKNOWLEDGED -> ROUTED_AT_VENUE
 ↓
 REJECTED
SENT -> REJECTED (local gateway reject)
ACKNOWLEDGED -> CANCELLED (MEIC/MLIC)
ACKNOWLEDGED -> MODIFIED -> ACKNOWLEDGED (MEIM/MLIM)
```

## 4. `order_modification.modification_status`

```
REQUESTED -> ACCEPTED -> APPLIED_TO_ORDER
REQUESTED -> REJECTED
```

**Phase 2d note:** `request_timestamp` captures customer request time (MEOMR); `event_timestamp` captures broker acceptance (MEOM). The gap between them is a regulator audit target.

## 5. `quote_request.request_status`

```
OPEN -> ANSWERED -> (junction row in quote_response)
OPEN -> EXPIRED (past expire_timestamp)
OPEN -> CANCELLED
```

## 6. `quote_event.quote_status`

```
ACTIVE -> EXECUTED (client hit; fires MEOT or MEOTQ)
ACTIVE -> SUPERSEDED (quote refresh; prior_quote_id populated)
ACTIVE -> CANCELLED
ACTIVE -> EXPIRED (past validity_end)
```

## 7. `request_for_execution.rfe_status`

```
RECEIVED -> ACCEPTED -> order_event created
RECEIVED -> REJECTED
RECEIVED -> EXPIRED
```

## 8. `indication_of_interest.ioi_status`

```
ACTIVE -> WITHDRAWN (withdrawn_timestamp set)
ACTIVE -> EXPIRED
```

## 9. `execution.execution_type` (FIX tag 150)

```
NEW -> PARTIAL* -> FULL (fill progression)
NEW -> BUST (reversal)
NEW -> CORRECT -> (amended) (correction via new row)
NEW -> CANCEL (fill cancelled)
```

## 10. `allocation.allocation_status`

```
PENDING -> CONFIRMED (MEAA)
PENDING -> REJECTED
PENDING -> AMENDED -> PENDING (loop; new allocation row)
PENDING -> CANCELLED
```

## 11. `trade_confirmation.confirmation_status` & `.match_status`

```
UNCONFIRMED -> AFFIRMED (client signs off - SEC Rule 15c6-2 same-day affirm target)
UNCONFIRMED -> DK (Don't Know) (counterparty rejects)
UNCONFIRMED -> DISPUTED
UNCONFIRMED -> CANCELLED

match_status: UNMATCHED -> MATCHED
match_status: UNMATCHED -> ALLEGED (only counterparty's side is present)
```

## 12. `clearing_record.clearing_status`

```
SUBMITTED -> ACCEPTED -> NOVATED
SUBMITTED -> REJECTED
```

**NOVATED** is terminal: CCP becomes the counterparty; original bilateral risk is extinguished.

## 13. `settlement_instruction.settlement_status`

```
INSTRUCTED -> MATCHED -> SETTLING -> SETTLED
INSTRUCTED -> MATCHED -> SETTLING -> PARTIAL
INSTRUCTED -> MATCHED -> SETTLING -> FAILED (CSDR penalty triggers)
INSTRUCTED -> UNMATCHED (counterparty alleges different)
```

## 14. `settlement_obligation.obligation_status`

```
PENDING -> NETTED -> SETTLED
PENDING -> NETTED -> FAILED (CNS fail; opens position_break)
```

## 15. `position.reconciliation_status`

```
UNRECONCILED -> IN_PROGRESS -> MATCHED
UNRECONCILED -> IN_PROGRESS -> BROKEN -> (opens position_break row)
```

## 16. `position_break.break_status`

```
OPEN -> INVESTIGATING -> RESOLVED
OPEN -> INVESTIGATING -> WRITTEN_OFF (materiality permits)
OPEN -> AGED (past materiality-specific aging threshold)
```

## 17. `agreement.agreement_status`

```
DRAFT -> EXECUTED -> ACTIVE -> TERMINATED
DRAFT -> EXECUTED -> ACTIVE -> AMENDED -> ACTIVE (loop; agreement_schedule captures)
DRAFT -> VOID
```

## 18. `margin_call.call_status`

```
ISSUED -> ACKNOWLEDGED -> PAID
ISSUED -> DISPUTED -> RESOLVED -> PAID
ISSUED -> EXPIRED (default trigger in CSA)
```

## 19. `regulatory_report.report_status`

```
GENERATED -> SUBMITTED -> ACCEPTED
GENERATED -> SUBMITTED -> REJECTED -> CORRECTED -> SUBMITTED -> ACCEPTED (loop)
```

## 20. `regulatory_hold.hold_status`

```
ACTIVE -> RELEASED
ACTIVE -> EXPIRED (hold_end_date reached)
```

---

## Cross-Entity Lifecycle Flow (happy path)

```
order_request(RECEIVED) -> order_event(NEW) -> order_route(SENT) -> execution(NEW -> PARTIAL -> FULL) -> allocation(PENDING -> CONFIRMED) -> trade_confirmation(UNCONFIRMED -> AFFIRMED, match_status=MATCHED) -> clearing_record(SUBMITTED -> ACCEPTED -> NOVATED) -> settlement_instruction(INSTRUCTED -> MATCHED -> SETTLING -> SETTLED) -> settlement_obligation(PENDING -> NETTED -> SETTLED) -> position(UNRECONCILED -> MATCHED)
```

## DLT Expectation Patterns

Every transition above should be enforced via a Delta Live Tables expectation with `@dlt.expect_or_fail` for invalid transitions, or `@dlt.expect_or_drop` if stale/duplicate rows should be dropped rather than failing the pipeline. Maintain a companion table `invalid_state_transitions` for ops review.
