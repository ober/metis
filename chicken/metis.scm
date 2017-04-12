(use z3)
(use medea)
(use vector-lib)
(use posix)
(use files)
(use srfi-13)

(define *fields* '(
		   "additionalEventData"
		   "awsRegion"
		   "errorCode"
		   "errorMessage"
		   "eventID"
		   "eventName"
		   "eventSource"
		   "eventTime"
		   "eventType"
		   "eventVersion"
		   "recipientAccountId"
		   "requestID"
		   "requestParameters"
		   "resources"
		   "responseElements"
		   "sourceIPAddress"
		   "userAgent"
		   "userIdentity"
		   "userName"
		   ))

(define (parse-json-gz-file file)
  (let* ((gzip-stream (z3:open-compressed-input-file file))
	 (json (read-json gzip-stream)))
    (close-input-port gzip-stream)
    json))

(define (parse-ct-file file)
  (parse-ct-contents file))

(define (parse-ct-contents file)
  (let* ((json (parse-json-gz-file file))
	 (entries (vector->list (cdr (car json)))))
    (for-each
     (lambda (x)
       (normalize-insert (process-record x)))
     entries)))

(define (normalize-insert record)
  (format #t "normalize-insert:~{ ~A ~}~%" record)
  )

(define (process-record line fields)
  ;;(format #t "line:~A~%" line)
  (for-each
   (lambda (x)
     (get-value x line))
   fields))

(define (ct-report-sync dir)
  (for-each
   (lambda (x)
     (cond ((string-suffix? ".json.gz" x)
	    (process-ct-file x))))
   (find-files dir)))

(define (get-value field record)
  (cond
   ((string= "additionalEventData" field) (assoc 'additionalEventData record))
   ((string= "awsRegion" field) (assoc 'awsRegion record))
   ((string= "errorCode" field) (assoc 'errorCode record))
   ((string= "errorMessage" field) (assoc 'errorMessage record))
   ((string= "eventID" field) (assoc 'eventID record))
   ((string= "eventName" field) (assoc 'eventName record))
   ((string= "eventSource" field) (assoc 'eventSource record))
   ((string= "eventTime" field) (assoc 'eventTime record))
   ((string= "eventType" field) (assoc 'eventType record))
   ((string= "eventVersion" field) (assoc 'eventVersion record))
   ((string= "recipientAccountId" field) (assoc 'recipientAccountId record))
   ((string= "requestID" field) (assoc 'requestID record))
   ((string= "requestParameters" field) (assoc 'requestParameters record))
   ((string= "resources" field) (assoc 'resources record))
   ((string= "responseElements" field) (assoc 'responseElements record))
   ((string= "sourceIPAddress" field) (assoc 'sourceIPAddress record))
   ((string= "userAgent" field) (assoc 'userAgent record))
   ((string= "userIdentity" field) (assoc 'userIdentity record))
   ((string= "userName" field) (assoc 'userName record))
   ))

(ct-report-sync "/Users/akkad/CT")
