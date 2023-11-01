def uppercase_decorator(function):
    def wrapper():
        func = function()
        make_uppercase = func.upper()
        return make_uppercase

    return wrapper

def split_decorator(function):
    def wrapper():
        fucn = function()
        split_list = fucn.split()
        return split_list

    return wrapper

#Decorator are called in order they are written.
@split_decorator
@uppercase_decorator
def say_hi():
    return "hello there"

x = say_hi()
print(x)