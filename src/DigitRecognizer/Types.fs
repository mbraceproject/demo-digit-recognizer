namespace DigitRecognizer

/// Image identifier
type ImageId = int

/// Image bitmap representation
type Image = { Id : ImageId ; Pixels : int [] }

/// Digit classification
type Classification = int

/// Distance on points; use uint64 to avoid overflows
type Distance = Image -> Image -> uint64

/// A training image annotaded by its classification
type TrainingImage = { Classification : Classification ; Image : Image }

/// Digit classifier implementation
type Classifier = TrainingImage [] -> Image -> Classification