class Handler:
    def __init__(self, methods=("GET",)):
        self.methods = methods

    def __call__(self, func, *args, **kwargs):
        def wrapper(request, *args, **kwargs):
            if request.get('method', "GET") in self.methods:
                if request.get('method', "GET") == "GET":
                    return self.get(func, request)
                elif request.get('method', "GET") == "POST":
                    return self.post(func, request)
            else:
                return None
        return wrapper

    def get(self, func, request, *args, **kwargs):
        return f"GET: {func(request)}"

    def post(self, func, request, *args, **kwargs):
        return f"POST: {func(request)}"


@Handler(methods=('GET', "POST"))  # по умолчанию methods = ('GET',)
def contact(request):
    return "Сергей Балакирев"


res = contact({"method": "POST", "url": "contact.html"})
print(res)
