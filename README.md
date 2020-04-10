# Universidad EAFIT
# Curso ST0263 Tópicos Especiales en Telemática, 2020-1
# Profesor: Edwin Montoya M. – emontoya@eafit.edu.co
# Profesor: Alvaro Ospina - aeospinas@eafit.edu.co

# Laboratorio MapReduce
## 1. Se tiene un conjunto de datos, que representan el salario anual de los empleados formales en Colombia por sector económico, según la DIAN.

* La estructura del archivo es: (sececon: sector económico) (archivo: dataempleados.csv)

      idemp,sececon,salary,year

      3233,1234,35000,1960
      3233,5434,36000,1961
      1115,3432,34000,1980
      3233,1234,40000,1965
      1115,1212,77000,1980
      1115,1412,76000,1981
      1116,1412,76000,1982

* Realizar un programa en Map/Reduce, con hadoop en Python o Java, que permita calcular:

1. El salario promedio por Sector Económico (SE)
2. El salario promedio por Empleado
3. Número de SE por Empleado que ha tenido a lo largo de la estadística

## 2. Ejecución del punto
[Lab MapReduce](/Lab%20MapReduce)

