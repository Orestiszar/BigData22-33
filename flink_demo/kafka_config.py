# base config for producers and consumers alike
conf = {
    'bootstrap.servers':"pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
    'security.protocol':"SASL_SSL",
    'sasl.mechanisms':"PLAIN",
    'sasl.username':"ITVCKFDO6EJFOGHD",
    'sasl.password':"BFmFxMtT1w9ciCxg99DnfI+YWR/Jv+nc4w3X50+4gkOToJJ7yUcRhR1HlkTyVMgM",

    # Best practice for higher availability in librdkafka clients prior to 1.7
    'client.id':''
}