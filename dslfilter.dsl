data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()


seldf = df.select("id" , "tdate")
seldf.show()

dropdf = df.drop("id", "tdate")
dropdf.show()

dft = df.filter("category= 'Exercise'")
print("*****Single filter***")
dft.show()

mulcol = df.filter("category= 'Exercise' or spendby= 'cash'")
print("*****MUlticolumn OR Filter***")
mulcol.show()

mulcol = df.filter("category= 'Exercise' and spendby= 'cash'")
print("*****MUlticolumn AND Filter***")
mulcol.show()

mulcol = df.filter("category in( 'Exercise', 'cash')")
print("*****MUltiValue Filter***")
mulcol.show()

contcol = df.filter("product like '%Gymnastics%'")
print("*****LIKE Filter***")
contcol.show()

nullfil= df.filter("product is null")
print("*****Null Filter***")
nullfil.show()


nullfil= df.filter("product is not NULL")
print("*****NotNull Filter***")
nullfil.show()
