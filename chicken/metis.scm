(use z3)
(use srfi-1)
(use medea)
(use vector-lib)
(use posix)
(use files)
(use srfi-13)
(use format)
(use list-bindings)



(define *db* (lmdb-open (make-pathname "." "mydb.mdb")))

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

(define (process-ct-file file)
  (parse-ct-contents file))

(define (parse-ct-contents file)
  (let* ((json (parse-json-gz-file file))
	 (entries (vector->list (cdr (car json)))))
    (for-each
     (lambda (x)
       (normalize-insert (process-record x '() *fields*)))
     entries)))

(define (normalize-insert record)
  (bind (
	 additionalEventData
	 awsRegion
	 errorCode
	 errorMessage
	 eventID
	 eventName
	 eventSource
	 eventTime
	 eventType
	 eventVersion
	 recipientAccountId
	 requestID
	 requestParameters
	 resources
	 responseElements
	 sourceIPAddress
	 userAgent
	 userIdentity
	 userName
	 )
	(reverse record)

	(format #t "NI: eventID:~A~%" eventID)))

(define (process-record line results fields)
  (cond ((null-list? fields)
	 results)
	(else
	 (let ((value (get-value (car fields) line)))
	   ;;(format #t "PR: field:~A value:~A~%" (car fields) value)
	   (process-record line
			   (cons value results)
			   (cdr fields))))))

(define (ct-report-sync dir)
  (for-each
   (lambda (x)
     (cond ((string-suffix? ".json.gz" x)
	    (process-ct-file x))))
   (find-files dir)))

(define (type-of x)
  (cond ((number? x) "Number")
	((list? x) "list")
	((pair? x) "Pair")
	((vector? x) "Vector")
	((null? x) "null")
	((string? x) "String")
	(else "Unknown type")))

(define (get-value field record)
  (let* ((field-sym (string->symbol field))
	(value (assoc field-sym record)))
    (cond ((pair? value)
	(cdr value))
	  (else value))))


(ct-report-sync "/Users/akkad/CT")

(lmdb-close *db*)
