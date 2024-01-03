
def split_decorator(function):
    def wrapper():
        func = function()
        print(f"printing from split {func}")
        split_list = func.split()
        return split_list

    return wrapper

def uppercase_decorator(function):
    def wrapper():
        func = function()
        print(f"printing from upppercase {func}")
        make_uppercase = func.upper()
        return make_uppercase

    return wrapper

#Decorator are called in order they are written.
@split_decorator
@uppercase_decorator
def say_hi():
    print("Executed from say hi!")
    return "hello there"

x = say_hi
print(x())