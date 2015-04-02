(*
Base types: 
* an Image is an array of 28 * 28 grayscale pixels (0..255)
* an Example is an image, with known Label (0 to 9)
* a Benchmark is an image we try to predict, with an ID but no Label
*)

type Image = int []
type Example = { Label: int; Image: Image }
type Benchmark = { ImageId: int; Image: Image }

open System.IO

let dataFolder = __SOURCE_DIRECTORY__ + "/../../data/"

let trainPath = dataFolder + "train.csv"
let testPath = dataFolder +  "test.csv"
let submissionPath = dataFolder + "submission.csv"

// Reading the 50,000 known examples in memory.

let training =
    trainPath
    |> File.ReadAllLines
    |> fun lines -> lines.[1..]
    |> Array.map (fun line -> line.Split ',' |> Array.map int)
    |> Array.map (fun line ->
        { Label = line.[0]; Image = line.[1..] })

// Euclidean distance between 2 images.
let size = 28 * 28
let distance (img1:Image) (img2:Image) =
    let rec compute acc i =
        if i = size then acc
        else
            let d = (img1.[i] - img2.[i]) * (img1.[i] - img2.[i])
            compute (acc + d) (i + 1)
    compute 0 0

// Given an image, find the label of the closest
// image from the 50,000 known training images.
let closest (examples:Example[]) img =
    examples
    |> Array.minBy (fun example -> distance example.Image img)
    |> fun closest -> closest.Label

// Read the 20,000 benchmark images we are trying
// to predict.

let testing =
    testPath
    |> File.ReadAllLines
    |> fun lines -> lines.[1..]
    |> Array.map (fun line -> line.Split ',' |> Array.map int)
    |> Array.mapi (fun i image -> 
        { ImageId = i + 1; Image = image } )

// Create a submission file:
// for each of the 20,000 images, produce
// a predicted label, and save the ImageId
// and prediction into a file.

let createSubmission =
    let predictions =
        testing
        |> Array.Parallel.map (fun test -> 
            sprintf "%i,%i" test.ImageId (closest training test.Image))
    [|
        yield "ImageId,Label"
        yield! predictions
    |]
    |> fun predictions -> File.WriteAllLines(submissionPath,predictions)

(*
Connect to Cluster
*)

#load "credentials.fsx"

open MBrace
open MBrace.Store
open MBrace.Workflows
open MBrace.Azure.Client
open System.IO

let logInfo message =
    local {
        let! worker = Cloud.CurrentWorker
        return! Cloud.Log <| sprintf "%s: %s" worker.Id message
    }

let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())
cluster.ShowWorkers()

type Cache = CloudCell

let test = cloud { return "hello" } |> cluster.Run
// Real: 00:00:18.095, CPU: 00:00:06.750, GC gen0: 6, gen1: 2, gen2: 0
let cachedTest = 
    cloud { 
        return! testing |> Cache.New }
    |> cluster.Run
// Real: 00:00:17.595, CPU: 00:00:06.656, GC gen0: 6, gen1: 1, gen2: 0
let cachedTrain =
    cloud {
        return! training |> Cache.New }
    |> cluster.Run

// with 2 machines
// Real: 00:20:53.543, CPU: 00:00:23.953, GC gen0: 435, gen1: 63, gen2: 5
// with 4 machines
// Real: 00:11:21.321, CPU: 00:00:16.000, GC gen0: 282, gen1: 32, gen2: 2
let cloudSubmission =
    cloud {
        let! train = Cache.Read cachedTrain
        let! test = Cache.Read cachedTest
        return!
            test
            |> DivideAndConquer.map (fun test -> 
                local {
                    return sprintf "%i,%i" test.ImageId (closest train test.Image) }) }
    |> cluster.Run
    |> Array.append [| "ImageId,Label" |]
    |> fun predictions -> File.WriteAllLines(submissionPath,predictions)

// alternate experiment - way worse than above            
let cloudSubmission2 =
    [ for test in testing -> 
        cloud {
            let! train = Cache.Read cachedTrain
            let classifier = closest train
            return sprintf "%i,%i" test.ImageId (classifier test.Image) } ]
    |> Cloud.Parallel
    |> cluster.Run
    |> Array.append [| "ImageId,Label" |]
    |> fun predictions -> File.WriteAllLines(submissionPath,predictions)