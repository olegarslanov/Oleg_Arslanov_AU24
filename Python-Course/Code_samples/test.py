def task_5():
    a, b = input("Vvedite dve cifry oi duhi cerez probel:").split()
    try:
        x = int(a)/int(b)
    except ZeroDivisionError:
        print("Can't divide by zero")
    except ValueError:
        print("Entered value is wrong")
    else:
        print(x)
    finally:
        print("Vsio")

task_5()