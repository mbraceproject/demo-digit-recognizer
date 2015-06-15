[<AutoOpen>]
module DigitRecognizer.Utils

open System
open System.IO
open System.IO.Compression
open System.Text
open Nessos.Streams
open MBrace.Core
open MBrace.Store
open MBrace.Core.Internals


module Gzip =
    let compress (stream : System.IO.Stream) = new GZipStream(stream, CompressionLevel.Optimal) :> Stream
    let decompress (stream : System.IO.Stream) = new GZipStream(stream, CompressionMode.Decompress) :> Stream
    let openWrite = File.OpenWrite >> compress
    let openRead = File.OpenRead >> decompress

type Stream =
    // quick'n'dirty implementation of missing Stream combinator
    /// Takes a System.IO.Stream returning a Nessos.Stream<string> containing all lines of text
    static member OfTextStream(stream : System.IO.Stream, ?encoding : Encoding, ?decompress : bool) : Stream<string> =
        seq { 
            let decompress = defaultArg decompress false
            let stream = if decompress then Gzip.decompress stream else stream
            use sr = match encoding with None -> new StreamReader(stream) | Some e -> new StreamReader(stream, e)
            while not sr.EndOfStream do yield sr.ReadLine () }
        |> Stream.ofSeq

type TrainingImage with

    /// Parses a training set from text using the Kaggle digit recognizer CSV format
    static member Parse(stream : System.IO.Stream, ?encoding : Encoding, ?decompress) : TrainingImage [] =
        Stream.OfTextStream(stream, ?encoding = encoding, ?decompress = decompress)
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> 
                            let id = i + 1
                            let image = { Id = id ; Pixels = nums.[1..] }
                            { Classification = nums.[0] ; Image = image })
        |> Stream.toArray

    /// Parses a training set from text using the Kaggle digit recognizer CSV format
    static member Parse(file : string, ?encoding : Encoding, ?decompress) : TrainingImage [] =
        use fs = File.OpenRead file
        TrainingImage.Parse(fs, ?decompress = decompress)

    /// Creates a cloud cell for a training image using supplied cloud file
    static member Parse(file : CloudFile, ?encoding, ?decompress, ?force) = local {
        let deserializer (s : System.IO.Stream) = TrainingImage.Parse(s, ?encoding = encoding, ?decompress = decompress) :> seq<_>
        return! CloudSequence.OfCloudFile(file.Path, deserializer, ?force = force)
    }


type Image with

    /// Parses a set of points from text using the Kaggle digit recognizer CSV format
    static member Parse (stream : System.IO.Stream, ?encoding : Encoding, ?decompress) : Image [] =
        Stream.OfTextStream(stream, ?encoding = encoding, ?decompress = decompress)
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> let id = i + 1 in { Id = id ; Pixels = nums })
        |> Stream.toArray

    /// Parses a set of points from text using the Kaggle digit recognizer CSV format
    static member Parse (file : string, ?encoding : Encoding, ?decompress) : Image [] =
        use fs = File.OpenRead file
        Image.Parse(fs, ?decompress = decompress)

    /// Creates a cloud sequence for a training image using supplied cloud file
    static member Parse(file : CloudFile, ?encoding, ?decompress, ?force) = local {
        let deserializer (s : System.IO.Stream) = Image.Parse(s, ?encoding = encoding, ?decompress = decompress) :> seq<_>
        return! CloudSequence.OfCloudFile(file.Path, deserializer, ?force = force)
    }


type Classifications =

    /// Writes a point classification to stream
    static member Write(stream : System.IO.Stream, classifications : (ImageId * Classification) [], ?encoding : Encoding, ?compress) =
        let stream = if defaultArg compress false then Gzip.compress stream else stream
        use sw = match encoding with None -> new StreamWriter(stream) | Some e -> new StreamWriter(stream, e)
        sw.WriteLine "ImageId,Label"
        classifications |> Array.iter (fun (i,c) -> sw.WriteLine(sprintf "%d,%d" i c))

    /// Writes a point classification to stream
    static member Write(outFile : string, classifications : (ImageId * Classification) [], ?encoding : Encoding, ?compress) =
        use fs = File.OpenWrite(outFile)
        Classifications.Write(fs, classifications, ?encoding = encoding, ?compress = compress)


module Balanced =
    let reduceCombine (reducer : 'T [] -> Local<'S>) (combiner : 'S [] -> Local<'R>) (inputs : 'T []) = cloud {
        let! workers = Cloud.GetAvailableWorkers()
        let! results =
            inputs
            |> WorkerRef.partitionWeighted (fun w -> w.ProcessorCount) workers
            |> Seq.map (fun (w,ts) -> reducer ts, w)
            |> Cloud.Parallel

        return! combiner results
    }