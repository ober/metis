(in-package :metis)

;; fundamental metis ops
(defun ssdb/init ()
  (unless ssdb:*connection*
    (ssdb:connect)))

(defun ssdb/close ()
  (ssdb:disconnect))

(defun ssdb/have-we-seen-this-file (file)
  (ssdb:exists (format nil "F-~a" (get-filename-hash file))))

(defun ssdb/mark-file-processed (file)
  (ssdb:set (format nil "F-~a" (get-filename-hash file)) "1"))

(defun ssdb/normalize-insert (record)
  (destructuring-bind (
                       additionalEventData
                       apiVersion
                       awsRegion
                       errorCode
                       errorMessage
                       eventCategory
                       eventID
                       eventName
                       eventSource
                       eventTime
                       eventType
                       eventVersion
                       managementEvent
                       readOnly
                       recipientAccountId
                       requestID
                       requestParameters
                       resources
                       responseElements
                       serviceEventDetails
                       sessionCredentialFromConsole
                       sharedEventID
                       sourceIPAddress
                       tlsDetails
                       userAgent
                       userIdentity
                       userName
                       vpcEndpointId
                       )
      record
    (ssdb:multi_hset eventID
                     "aed" additionalEventData
                     "av" apiVersion
                     "ar" awsRegion
                     "ec" errorCode
                     "em" errorMessage
                     "eca" eventCategory
                     "en" eventName
                     "es" eventSource
                     "et" eventTime
                     "typ" eventType
                     "ev" eventVersion
                     "me" managementEvent
                     "ro" readOnly
                     "rai" recipientAccountId
                     "rqi" requestID
                     "rp" requestParameters
                     ;;"res" resources
                     "re" responseElements
                     "sed" serviceEventDetails
                     "scf" sessionCredentialFromConsole
                     "sei" sharedEventID
                     "sia" sourceIPAddress
                     "td" tlsDetails
                     "ua" userAgent
                     ;;"ui" userIdentity
                     "un" userName
                     "vpc" vpcEndpointId)))

;; ported kunabi style ops

(defun ssdb/db-key? (key)
  (ssdb:exists key))

(defun ssdb/db-get (key)
  (ssdb:get key))

(defun ssdb/get-stats ()
  (format t "~a" (ssdb:info)))

(defun ssdb/get-unique-region ()
  (ssdb/get-unique "ar"))

(defun ssdb/get-unique-ip ()
  (ssdb/get-unique "sia"))

(defun ssdb/get-unique-agent ()
  (ssdb/get-unique "ua"))

(defun ssdb/get-unique-names ()
  (ssdb/get-unique "un"))

(defun ssdb/get-unique-events ()
  (ssdb/get-unique "en"))

(defun ssdb/get-unique (key)
  (let ((hits (sort-uniq (ssdb:qrange key 0 -1))))
    (mapcar
     (lambda (hit)
       (format t "~a~%" hit))
     hits)))

(defun ssdb/fetch-print-hash (hit)
  (format t "~a~%" (ssdb:multi_hget hit "et" "en" "un" "ec")))

(defun add-item-to-list (item list)
  (if (member item list)
      list
      (cons item list)))

(defun ssdb/index (field)
  (let ((records (time (ssdb:hlist "" "" -1)))
        (seen '()))
    (format t "records: ~a~%" (length records))
    (mapcar
     (lambda (record)
       (let ((value (ssdb:hget record field)))
         (if (member value seen)
             (format t "seen: ~a~%" value)
             (progn
               (setf seen (add-item-to-list value seen))
               (ssdb:qpush field value)))
             (ssdb:qpush value record)))
     records)))

(defun ssdb/get-by-index (key)
  "Get all records from index of key"
  (let ((hits (sort-uniq (ssdb:qrange key 0 -1))))
    (mapcar
     (lambda (hit)
       (ssdb/fetch-print-hash hit))
     hits)))

(defun ssdb/uniq-queues ()
  "For each queue, go fetch its contents uniq them and put them back"
  (let ((queues (ssdb:qlist "" "" -1)))
    (format t "queues: ~A~%" (length queues))
    (mapcar
     (lambda (q)
       (let* ((items (ssdb:qrange q 0 -1))
              (uniqs (sort-uniq items)))
         (format t "q:~a size:~a uniq:~a~%" q (length items) (length uniqs))
         (ssdb:qclear q)
         (ssdb:qpush q uniqs)))
     queues)))
