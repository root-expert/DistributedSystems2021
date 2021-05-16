# Κατανεμημένα Συστήματα Πρώτο Παραδοτέο

### Απαιτήσεις:
1. Gradle 7.0.0
2. Java  SE 8
### Arguments:
* broker: Δημιουργεί ένα broker instance. 
* node: Δημιουργεί ένα appnode instance.


### Πως να τρέξει:
1. Ανοίξτε το τοπικό Terminal
2. Τρέξτε την εντολή `gradle task build`
3. Δημιουργήστε το ShadowJar με  `gradle shadowjar`.
   
   Το jar artifact βρίσκεται στο φάκελο /build/libs/.
4. Για κάθε Broker και Appnode δημιουργήστε ένα καινούργιο φάκελο και τοποθετήστε το jar artifact σε αυτόν.
5. Άμα είναι Broker τότε τοποθετήστε το broker.yml (από τον φάκελο /config/) στο φάκελο, ενώ αν είναι Appnode το appnode.yml.
6. Αλλάξτε τις ρυθμίσεις των YAML αρχείων όπως σας βολεύει.
7. Ανοίξτε Terminal για κάθε φάκελο που δημιουργήσατε και τρέξτε την εντολή `java -jar <jar-filename> <broker|node>` ανάλογα με το αν είναι broker ή  appnode.


### Σημείωση:
Για να αναγνωριστούν τα βίντεο από το πρόγραμμα πρέπει να βρίσκονται στην μορφή "(name)(hashtags with #).mp4".

Για παράδειγμα το αρχείο "World#Settings#html.mp4", όπου το World αποτελεί το όνομα  και το βίντεο ανεβαίνει με τα hashtag #Settings και #html.

Για να διευκρινήσετε ότι ένα video ανήκει σε ένα appnode πρέπει να υπάρχει στον φάκελο, που δημιούργήσατε, στο οποίο υπάρχει το jar artifact.

Τα βίντεο που δέχονται οι appnodes θα αποθηκεύονται σε ένα τοπικό φάκελο /out/ που δημιουργεί το πρόγραμμα.


### Τα video  που χρησιμοποιήσαμε 
https://drive.google.com/file/d/1vKknigwHjSRXNnbdCKoWO0E3jIbM7onz/view?usp=sharing
https://drive.google.com/file/d/1HkD6Ss4Ha2vBfOFZcgHVTL0PpkeTLE17/view?usp=sharing
https://drive.google.com/file/d/10_lplCZ8-S-XzkBx9sXPXD1EMRzm6rcg/view?usp=sharing
https://drive.google.com/file/d/1q0ZCkiwrvPMvl9mlSKPbnQ6NZuK7BJKZ/view?usp=sharing