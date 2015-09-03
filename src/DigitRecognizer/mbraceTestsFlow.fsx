#load "credentials.fsx"
#I "../../bin"
#r "Streams.Core.dll"
#r "MBrace.Flow.dll"
#r "DigitRecognizer.dll"

open MBrace.Core
open MBrace.Flow
open MBrace.Workflows
open MBrace.Azure.Client

open Nessos.Streams

open DigitRecognizer
open DigitRecognizer.Knn

// First connect to the cluster
let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())

// initialize a local standalone cluster
// let cluster = Runtime.InitLocal(config, workerCount = 2)

// attach a local worker to cluster
// cluster.AttachLocalWorker()

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

// parse data
let training = TrainingImage.Parse trainPath
let tests = Image.Parse testPath

let classify (classifier : Classifier) (images : Image []) =
    CloudFlow.OfArray images
    |> CloudFlow.map (fun img -> img.Id, classifier training img)
    |> CloudFlow.toArray

let validate (classifier : Classifier) (validation : TrainingImage []) = cloud {
    let! successCount =
        CloudFlow.OfArray validation
        |> CloudFlow.filter (fun tI -> classifier training tI.Image = tI.Classification)
        |> CloudFlow.length

    return float successCount / float validation.Length
}