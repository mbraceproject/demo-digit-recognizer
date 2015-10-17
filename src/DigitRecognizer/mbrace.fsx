#load "../../packages/MBrace.Azure/MBrace.Azure.fsx"
#I "../../bin/"
#r "Streams.Core.dll"
#r "MBrace.Flow.dll"
#r "DigitRecognizer.dll"

open MBrace.Core
open MBrace.Flow
open MBrace.Library
open MBrace.Azure

open DigitRecognizer
open DigitRecognizer.Knn

// Install Azure connection strings info to your local environment
//Configuration.EnvironmentStorageConnectionString <- "your Azure storage connection string"
//Configuration.EnvironmentServiceBusConnectionString <- "your Azure service bus connection string"

let config = Configuration.FromEnvironmentVariables()
let cluster = AzureCluster.Connect(config, logger = new ConsoleLogger(), logLevel = LogLevel.Debug)

// initialize a local standalone cluster
// let cluster = MBraceCluster.InitOnCurrentMachine(config, workerCount = 2)

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

// parse data
let training = TrainingImage.Parse trainPath
let tests = Image.Parse testPath

// CloudFlow implementations
let validate (classifier : Classifier) (training : TrainingImage []) (validation : TrainingImage []) = cloud {
    let! successCount =
        CloudFlow.OfArray validation
        |> CloudFlow.filter (fun tI -> classifier training tI.Image = tI.Classification)
        |> CloudFlow.length

    return float successCount / float validation.Length
}

let classify (classifier : Classifier) (training : TrainingImage []) (images : Image []) =
    CloudFlow.OfArray images
    |> CloudFlow.map (fun img -> img.Id, classifier training img)
    |> CloudFlow.toArray

let classifier = knn l2 10

// 1. Validation operation
let validateProc = cloud { return! validate classifier training.[0 .. 39999] training.[40000 ..] } |> cluster.CreateProcess

// 2. Send Classify job
let classifyProc = cloud { return! classify classifier training tests } |> cluster.CreateProcess

cluster.ShowWorkers()
cluster.ShowProcesses()