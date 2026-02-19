# Proptech MVP Workflow Chart

## What this MVP solves

1. Reduce dependency on brokers
2. Faster lead-to-visit execution
3. Better broker accountability
4. Better RM control and visibility

## End-to-end flow

```mermaid
flowchart LR
    A[Lead from Excel] --> B[App DB Lead Sync Every 30 Min]
    B --> C[RM Matches Top Properties]
    C --> D[Customer Shortlists on WhatsApp]
    D --> E[Customer Books Slot]
    E --> F[Visit Scheduled with Broker]
    F --> G[RM D-1 Reminder]
    G --> H[Visit Happens]
    H --> I[OTP + Geo Check-In or Photo Fallback]
    I --> J[Visit Completed]
    J --> K[Unique/Non-Unique Counters]
    K --> L[Reports + Exports]

    F --> M[Broker Cancels Slot]
    M --> N{Within 24h and booked?}
    N -- Yes --> O[Customer Apology + Priority Rebook 48h + RM Call Trigger]
    O --> P{Emergency Claimed?}
    P -- Yes --> Q[RM Review SLA 12/24h]
    Q --> R{RM Approves?}
    R -- No --> S[Flag Applied]
    Q --> T[Missed SLA -> SRM Escalation]
    T --> U{SRM Approves?}
    U -- No --> S
    P -- No --> S
    N -- No --> V[Apology Only]

    S --> W[Flag 1 Warning]
    W --> X[Flag 2 + Incentive Block Marker]
    X --> Y[Flag 3 Broker Deactivated]
    S --> Z[Each flag decays in 90 days]

    D --> AA[Customer Self-Service]
    AA --> AB[Cancel Visit]
    AA --> AC[Reschedule Visit]
```

## Role-wise screens

1. Broker
- Inventory add/remove
- Slot calendar
- Scheduled visits
- OTP send and completion

2. RM
- Duplicate review queue
- Emergency approval queue
- WhatsApp logs and template test send
- Funnel and reliability reports
- CSV exports

3. SRM
- Escalation queue and emergency decision

4. Customer self-service
- Load visits by phone
- Cancel or reschedule

## Verification and policy rules

1. Duplicate listings
- Similarity above 75%: hidden from customer + RM review
- Similarity above 95%: auto-hidden + RM review

2. Visit completion
- OTP valid for 2 minutes, max 3 attempts
- Geo radius 200 meters
- Photo fallback if GPS fails

3. Cancellation policies
- Broker cannot reject customer directly, only slot control
- Booked cancel within 24h triggers strict workflow

4. Lifecycle accounting
- Unique visit = customer's first completed lifetime visit
- Non-unique tracked separately

## Sharing note

This file can be shared directly as Markdown, copied to Notion, or exported as PDF for non-technical stakeholders.
