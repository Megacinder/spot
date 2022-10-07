class Layer:
    def __init__(self, next_layer=None, name='Layer'):
        self.next_layer = next_layer
        self.name = name

    def __call__(self, *args, **kwargs):
        layer = args[0]
        self.next_layer = layer
        return layer


class Input(Layer):
    def __init__(self, inputs):
        self.inputs = inputs
        super().__init__(name='Input')


class Dense(Layer):
    def __init__(self, inputs, outputs, activation):
        self.inputs = inputs
        self.outputs = outputs
        self.activation = activation
        super().__init__(name='Dense')


class NetworkIterator:
    def __init__(self, network):
        self.network = network

    def __iter__(self):
        self.network.text_layer = self.network.next_layer
        return self

    def __next__(self):
        if self.network:
            node = self.network
            self.network = self.network.next_layer
            return node
        else:
            raise StopIteration


network = Input(128)
layer = network(Dense(network.inputs, 1024, 'linear'))
layer = layer(Dense(layer.inputs, 10, 'softmax'))
for x in NetworkIterator(network):
    print(x.name)
