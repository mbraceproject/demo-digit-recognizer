(*
Replicating the Naive Baseline, running computations on mbrace.
"Simplest possible model": 1-nearest neighbor, euclidean distance.
*)

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

(*
Connect to Cluster
*)

#load "credentials.fsx"

open MBrace
open MBrace.Store
open MBrace.Workflows
open MBrace.Azure.Client

let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())

let logInfo message =
    cloud {
        let! worker = Cloud.CurrentWorker
        return! Cloud.Log <| sprintf "%s: %s" worker.Id message
    }

let trainFileName = "train.csv"
let testFileName = "test.csv"
let submissionFileName = "submission.csv"

let localDataFolder = __SOURCE_DIRECTORY__ + "/../../data/"
let localPath (fileName:string) = localDataFolder + fileName

// move csv files to cluster storage

let mbraceDataFolder = cluster.DefaultStoreClient.FileStore.File

let upload (fileName:string) =
    match mbraceDataFolder.Enumerate() |> Seq.tryFind(fun file -> file.Path.Contains fileName) with
    | Some file -> file
    | None -> mbraceDataFolder.Upload (localPath fileName)

let cloudTrain = upload trainFileName
let cloudTest = upload testFileName

// Reading the 50,000 known examples in memory.

let buildTrainingSet =
    cloud {
        //TODO: Cache this later
        do! logInfo "Reading training set..."
        let! lines = 
            cloudTrain.Path
            |> CloudFile.ReadAllLines
        do! logInfo <| sprintf "Read (%d) lines" lines.Length

        let training =
            lines
            |> fun lines -> lines.[1..]
            |> Array.map (fun line -> line.Split ',' |> Array.map int)
            |> Array.map (fun line ->
                { Label = line.[0]; Image = line.[1..] })

        do! logInfo "Built training set."
        return training
    }

let predict (trainingSet:Example []) =
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
    fun img ->
        trainingSet
        |> Seq.minBy (fun example -> distance example.Image img)
        |> fun closest -> closest.Label

// Create a submission file:
// for each of the 20,000 images, produce
// a predicted label, and save the ImageId
// and prediction into a file.
#time

let createSubmission =
    cloud {
        do! logInfo "Reading test set..."
        let! lines = cloudTest.Path |> CloudFile.ReadAllLines
        do! logInfo "Read test set."
        let cloudImages =
            lines.[1..]
            |> Array.map (fun line -> line.Split ',' |> Array.map int)
            |> Array.mapi (fun i image -> 
                { ImageId = i + 1; Image = image } )
        do! logInfo "Built unknown images."
        return!
            cloudImages.[ .. 10]
            |> Array.map (fun test -> 
                cloud { 
                    do! logInfo <| sprintf "Predicting ImageId %d" test.ImageId
                    let! trainingSet = buildTrainingSet
                    let prediction = predict trainingSet test.Image
                    do! logInfo <| sprintf "Predicted %d" prediction
                    return test.ImageId, prediction })
            |> Cloud.Parallel
    } |> cluster.CreateProcess

createSubmission.GetLogs()
cluster.GetProcess "4d5c065f51b94f3c9ac47fdba7fcb59d" |> fun x -> x.ShowLogs()
//    [|
//        yield "ImageId,Label"
//        yield! predictions
//    |]
//    |> fun predictions -> File.WriteAllLines(submissionPath,predictions)