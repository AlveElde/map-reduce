# MapReduce

### Intro
MapReduce er et rammeverk funnet opp av Google i sine tidlige stadier for å forenkle prosessen å fordele oppgaver på maskiner som ellers ville tatt for lang tid på en. 

Vi snakker oppgaver som ville tatt en måned eller to blir utført på et par dager.

På den tiden brukte de vanlige maskiner med to trege kjerner, litt minne og en IDE snurredisk, alt på et nettverk som ikke nødvendigvis hadde gigabit en gang. 

I tillegg var det hyppige problemer med maskiner som krasjer, disker som kneler, nettverk faller ut og så videre, for ikke å snakke om at disse maskinene var en delt ressurs for alle i google. 

For en bruker internt i Google, det å skrive et eget program som kunne spre seg ut over dette clustered selv tok lang tid og et eksempel med 3800 linjer med kode. Og hvordan skulle man håndtere failures? Ikke akkurat et ikke-trivielt problem.

Derav MapReduce. Målet her var at brukeren skulle kun skrive hva som måtte til for å løse sitt problem, og sitte igjen med 700 linjer med kode i samme eksempel som sist. MapReduce står for å håndtere skaleringen. 

For å gjøre dette skriver brukeren to deler av programmet: en map del og en reduce del. Detaljene rundt disse delene kommer vi tilbake til. 

### Kjøring

#### Dette er da "User Program" på grafen.

MapReduce assigner en maskin som "Master:" denne prosessen fordeler oppgaver, holder styr på hvilken maskin som gjør hva og mest kritisk av alt: holder styr på failures. Kort forklart: en oppgave blir market som 'uferdig' frem til maskinen som skal utføre den er ferdig. Om Master ikke klarer å pinge en maskin etter en viss tid, eller at prosessen ikke er ferdig etter en slags "worst case tid" antar den maskinen som død og assigner en ny maskin til oppgaven. Dette fungere også om selve prosessen krasjer og maskinen er oppe: den får oppgaven på nytt. 

#### Første steg: map
Her for arbeiderene hver sin split av dataen som skal jobbes på og lager en liste av <key, value> par.

#### Mellom-steg: combine/shuffle
Data blir satt sammen på et eller annet hvis om nødvendig. Meningsløst å ha en liste av <term, 1> på et dokument når man kan én <term, termfrekvens> for det dokumentet om dette er en trivielt å løse.
Dette skjer på maskinen som har utført map og kan spare en god del nett-trafikk. 

#### Reduce
Alle reduce prosesser jobber med alle par med en key, slik at det blir enklere å jobbe med store mengder data som ikke passer i minne. 

#### Resultat
Må nok slåes sammen fra flere filer over nettverk, men dette skal være trivielt. 


### Eksempler:
Distributed Grep / Sort, og diverse telling. Vi bruker Inverted Index som eksempel.
