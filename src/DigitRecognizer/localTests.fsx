#I "../../bin"
#r "DigitRecognizer.dll"

open DigitRecognizer
open DigitRecognizer.Knn

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

let training = TrainingImage.Parse trainPath
let test = Image.Parse testPath

let training' = training.[ .. 39999]
let validation = training.[40000 ..]

let classifier = knn l2 10

#time

// Performance (quad core i7 cpu)
// Real: 00:15:30.855, CPU: 01:56:59.842, GC gen0: 2960, gen1: 2339, gen2: 1513
classifyLocalMulticore classifier training test

// Performance (quad core i7 cpu)
// Real: 00:01:02.281, CPU: 00:07:51.481, GC gen0: 179, gen1: 82, gen2: 62
validateLocalMulticore classifier training' validation