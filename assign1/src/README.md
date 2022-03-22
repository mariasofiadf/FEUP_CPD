Correr antes de usar PAPI:

```
sudo sh -c 'echo -1 >/proc/sys/kernel/perf_event_paranoid'
```

Compile C++: 

```
g++ -O2 matrixproduct.cpp -o matrixproduct -lpapi
```

Run C++:

```
./matrixproduct
```


Compile Java

```
javac matrixproduct.java
```

Run Java

```
java MatrixProduct
```
