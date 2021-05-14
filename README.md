# Distributed Systems First Distibutable

### Requirements:
1. Gradle 7.0.0
2. Java  SE 8
### Arguments:
* broker: Δημιουργεί ένα broker instance. 
* node: Δημιουργεί ένα appnode instance.
### Πως να τρέξει:
1. Ανοίξτε το τοπικό Terminal
2. Τρέξτε την εντολή `grdle task build`
3. Δημιουργήστε το ShadowJar με  `gradle shadowjar`.
   
   Το jar artifact βρίσκεται στο φάκελο /build/libs/.
4. Για κάθε Broker και Appnode δημιουργήστε ένα καινούργιο φάκελο και τοποθετήστε το jar artifact σε αυτόν.
5. Άμα είναι Broker τότε τοποθετήστε το broker.yml (από τον φάκελο /config/) στο φάκελο, ενώ αν είναι Appnode το appnode.yml.
6. Αλλάξτε τις ρυθμίσεις των YAML αρχείων όπως σας βολεύει.
7. Ανοίξτε Terminal για κάθε φάκελο που δημιουργήσατε και τρέξτε την εντολή `java -jar <jar-filename> <broker|node>` ανάλογα με το αν είναι broker ή  appnode.
