# SQL Discussion form

## Administration
* Your name: Orfeas

* Your discussion partner's name: Eefkje-femke

* Commit hash of the code you discussed: 2c863c9

* Your code's pylint score: 8.44

## Discussion
### Pre-set discussion points:
- Start by discussing your DB design choices (tables, data types)

I have the following tables:

Proteins
Species
Species_protein

My implementation uses mostly varchars, Eefkje uses text, which is more applicable.
We both should've looked at each column/feature and determined which datatype is more appropriate.
Our tables do differ. Eefkje uses A Species, Gene and Protein tables.

**How did you split up the work (functions/classes)?**

I have the following tables, storing the following columns/features:
1. Species
   1. Primary Key ID
   2. Species Name
   3. Accesion ID
   4. Genome Size
   5. Number of genes
   6. Number of proteins
   7. TaxDB ID
   
2. Proteins
   1. Primary key ID
   2. Protein ID
   3. Locus
   4. Locus tag
   5. Gene Reference ID
   6. EC Num
   7. GO Annotation
   8. GO FUnction
   
3. Species_protein
   1. Primary key ID
   2. Protein ID 
   3. Species ID

**How long (about) does it take, where is time spent?**

I have not looked at individual fucntion calls, but the entire script takes about 6 minutes to drop tables, create tables and insert rows. Eefkje's implementation takes about 10 minutes. Think this difference is maninly because my script inserts rows in batches and Eefkje's script inserts each row individually.

**Did you use parallel processing, if so how? If not, why not?**

I have not used parallel processing, solely because I ran out of time. Neither did Eefkje. However, I did try and make the script with future multiprocessing in mind. With MPI and SeqIO indexing I should be ablet to divide batches.

**What was your difference in =pylint= scores? Can you see why?**

0.6 Difference. It seems that we used different variable namimg conventions. Some of my lines are also too long. Eefkje does not have this problem.

**How did the normalization level (table number) influence the code?**

We both have level 3 normalization. However, my code doesn't use foreign keys and Eefkje's code uses foreign keys.

**Did your choice in database table design make it more or less hard to do the assignment?**

It was pretty doable in both cases. However, because of the nature of my species_Protein database, I have to do more insertions
than Eefkje's implementation. My implementation is less stringent, but and potentially less informative. Eefkje's implementaiton is more transactional based.

### Extra points that came up during discussion of your code:
We both used Python Black. We both used SeqIoO, and experienced a steep learning curve using the package.

