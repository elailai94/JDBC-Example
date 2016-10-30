all:
	@echo "Compiling..."
	javac JDBCExample.java

run: all
	@echo "Running..."
	java -cp 'postgresql-9.4.1208.jre6.jar:.' JDBCExample postgres.student.cs 5432 ahlai ahlai ootacamund emp.csv 50000

clean:
	@echo "Cleaning..."
	rm *.class
