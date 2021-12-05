# pythonprogramming-workerpool
WorkerPool (A-11) project for Python Programming course @ UAIC Iasi.

## Project Description

Dezvoltati un asamblu de descarcare pagini web ce va fi format dintr-un script master
(ce va programa primele 500 de pagini din fiecare tara conform alexa: https://www.alexa.com/topsites/countries)
si un script worker (script ce va descarca continutul paginilor).
Master.py va scrie intr-o coada (redis sau rabbitmq) informațiile despre paginile ce trebuiesc
descarate, iar worker.py (pot fi mai multe instanțe) va prelua din acea coada informațiile și va face descarcarea.
Pentru fiecare pagina ce trebuie descarcata master.py va stoca in coada un json cu urmatoarele informatii:

- Link
- LocatieDisk ( folderul in care se va salva link-ul )

### Input

Coada de redis/rabbit.

### Output

Fisierele descarcate in locatiile specificate

Logurile programelor worker.py si master.py precum si erorile aparute
