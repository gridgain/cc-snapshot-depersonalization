## The utility for depersonalization CC snapshots.

Changes data in 

1. AccountCache
2. NotificationConfigurationCache
3. ResourceMetadataCache
4. TeamCache


Motifies fields

1. email
2. tag
3. firstName
4. lastName
5. company
6. phone
7. fileName
8. recipients
9. name

Replacing rules:

1. vowels -> 'a'
2. consonants -> 'b'
3. digits -> '1'
4. prices and customers -> random prices and customers from env variables


To run, set env variables (these customers and prices are from the `cloud-qa` Stripe project):

```
DEPERSONALIZATION_OUTPUT_DIRECTORY=/path/to/input/snapshot/dir
DEPERSONALIZATION_INPUT_DIRECTORY=/path/to/output/snapshot/dir
DEPERSONALIZATION_REPLACEMENT_NEBULA_PRICES=price_1JUtowGYBz9hKSbVkfgaUxFQ,price_1JUtmRGYBz9hKSbVpHuPeAdT,price_1JUtkBGYBz9hKSbVUFAt7SiD
DEPERSONALIZATION_REPLACEMENT_CUSTOMERS=cus_L9P6kPfzH4xK2l,cus_L8IHcqcHTalrxP,cus_L46FvIbcnRdqvo,cus_KzBBgwMAkZk45T,cus_KyqHe5jL4yCsdL,cus_KyuVFxr05oIUSb
```
