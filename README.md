# Thrift support in WarpScript

This extension adds the possibility from within WarpScript to serialize and deserialize data using the [Thrift](https://thrift.apache.org/) format.

The traditional workflow when working with Thrift is to generate code for your target language using a `.thrift` IDL file as input. Then within your application the generated structure can be manipulated and a set of utility classes can be used to perform serialization and deserialization.

The approach adopted in WarpScript is similar except the code generation step has been removed. Instead the function `THRIFTC` can be used to generate an internal WarpScript structure that can be fed as input to serialization (`->THRIFT`) and deserialization (`THRIFT->`) functions. This approach renders the use of Thrift completely dynamic, enabling runtime generation of serialization/deserialization templates.

To the best of our knowledge this is the first attempt at a fully dynamic use of Thrift in any data environment. No doubt this feature will have many fans!
