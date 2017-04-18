(use args)
(use files)
(use format)
(use list-bindings)
(use lmdb)
(use medea)
(use posix)
(use s11n)
(use srfi-1)
(use srfi-13)
(use srfi-19)
(use vector-lib)
(use z3)
(use natural-sort)

(define *db* (lmdb-open (make-pathname "/home/ubuntu/" "metis.mdb") mapsize: 100000000000))

;;(define *db* (lmdb-open (make-pathname "/Users/akkad/" "metis.mdb") key: (string->blob "omg") mapsize: 1000000000))

(define *fields* '(
		   "additionalEventData" ;; 1
		   "awsRegion" ;; 2
		   "errorCode" ;; 3
		   "errorMessage" ;; 4
		   "eventID" ;; 5
		   "eventName" ;; 6
		   "eventSource" ;; 7
		   "eventTime" ;; 8
		   "eventType" ;; 9
		   "eventVersion" ;; 10
		   "recipientAccountId" ;; 11
		   "requestID" ;; 12
		   "requestParameters" ;; 13
		   "resources" ;; 14
		   "responseElements" ;; 15
		   "sourceIPAddress" ;; 16
		   "userAgent" ;; 17
		   "userIdentity" ;; 18
		   "userName" ;; 19
		   ))

(define (parse-json-gz-file file)
  (let* ((gzip-stream (z3:open-compressed-input-file file))
	 (json (read-json gzip-stream)))
    (close-input-port gzip-stream)
    json))

(define (process-ct-file file)
  (time (parse-ct-contents file)))

(define (parse-ct-contents file)
  (let* ((btime (current-seconds))
	 (json (parse-json-gz-file file))
	 (entries (vector->list (cdr (car json))))
	 (length (list-length entries)))
    (for-each
     (lambda (x)
       (normalize-insert (process-record x '() *fields*)))
     entries)
    (format #t " entries: ~A " length)))


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

	(let ((key (string-join (list eventTime eventName eventSource) "-"))
	      (value (list additionalEventData awsRegion errorCode errorMessage eventID eventName eventSource eventType eventVersion recipientAccountId requestID requestParameters resources responseElements sourceIPAddress userAgent userIdentity userName)))
	  (lmdb-set! *db*
		     (string->blob (->string key))
		     (string->blob (with-output-to-string
				     (cut serialize value))))
	)))

(define (process-record line results fields)
  (cond ((null-list? fields)
	 results)
	(else
	 (let ((value (get-value (car fields) line)))
	   (process-record line
			   (cons value results)
			   (cdr fields))))))

(define (ct-report-sync dir)
  (let ((i 0))
    (lmdb-begin *db*)
    (for-each
     (lambda (x)
       (cond ((string-suffix? ".json.gz" x)
	      (begin
		(process-ct-file x)
		(set! i (+ i 1))
		(when (eq? (modulo i 100) 0)
		  (lmdb-end *db*)
		  (lmdb-begin *db*))
	       ))))
     (find-files dir follow-symlinks: #t)))
  (lmdb-end *db*))

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

(define list-length
  (lambda (obj)
    (call-with-current-continuation
      (lambda (return)
        (letrec ((r
                  (lambda (obj)
                    (cond ((null? obj) 0)
                          ((pair? obj)
                           (+ (r (cdr obj)) 1))
                          (else (return #f))))))
          (r obj))))))

(define (get-field-num op)
  (format #t "get-field-num: ~A~%" (string? op))
  (cond ((string= "additionalEventData" op) 0)
	((string= "awsRegion" op) 1)
	((string= "errorCode" op) 2)
	((string= "errorMessage" op) 3)
	((string= "eventID" op) 4)
	((string= "eventName" op) 5)
	((string= "eventSource" op) 6)
	((string= "eventTime" op) 7)
	((string= "eventType" op) 8)
	((string= "eventVersion" op) 9)
	((string= "recipientAccountId" op) 10)
	((string= "requestID" op) 11)
	((string= "requestParameters" op) 12)
	((string= "resources" op) 13)
	((string= "responseElements" op) 14)
	((string= "sourceIPAddress" op) 15)
	((string= "userAgent" op) 16)
	((string= "userIdentity" op) 17)
	((string= "userName" op) 18)
	(else 0)))

(define (get-all-eventnames)
  (lmdb-begin *db*)
  (let ((results '()))
    (for-each
     (lambda (key)
       (let* ((ourkey (blob->string key))
	     (ourevent (list-ref (string-split ourkey "-") 3)))
	 (unless (member ourevent results)
	   (set! results (cons ourevent results)))))
       (lmdb-keys *db*))
    (lmdb-end *db*)
    (for-each
     (lambda (x)
       (format #t "~A~%" x))
     (natural-sort results))))

(define (get-by-eventname eventname)
  (lmdb-begin *db*)
  (for-each
   (lambda (key)
     (let* ((ourkey (blob->string key))
	    (ourevent (list-ref (string-split ourkey "-") 3)))
       (if (string= ourevent eventname)
	   (format #t "~A~%" (with-input-from-string
				 (blob->string (lmdb-ref *db* key))
			       (cut deserialize))))))
   (lmdb-keys *db*))
  (lmdb-end *db*))

(define (get-by-username username)
  (lmdb-begin *db*)
  (for-each
   (lambda (key)
     (let* ((ourkey (blob->string key))
	    (ourevent (list-ref (string-split ourkey "-") 3)))
       (if (string= ourevent eventname)
	   (format #t "~A~%" (with-input-from-string
				 (blob->string (lmdb-ref *db* key))
			       (cut deserialize))))))
   (lmdb-keys *db*))
  (lmdb-end *db*))

(define opts
  (list
   (args:make-option (l load) (required: "DIR") "Load Cloudtrail files in directory" (ct-report-sync arg))
   (args:make-option (so) (required: "OP") "Return all records of eventType OP" (show-ops arg))
   (args:make-option (lev) #:none "List all event types." (get-all-eventnames))
   (args:make-option (sev) (required: "ENV") "Return all records of eventType." (get-by-eventname arg))
   (args:make-option (c) #:none "Get Entry Count." (begin
						     (lmdb-begin *db*)
						     (format #t "count:~A~%" (lmdb-count *db*))
						     (lmdb-end *db*)))
   (args:make-option (sev) (required: "ENV") "Search by eventName." (get-by-eventname arg))
   (args:make-option (h help) #:none "Display this text" (usage))))

(define (usage)
 (with-output-to-port (current-error-port)
   (lambda ()
     (print "Usage: " (car (argv)) " [options...] [files...]")
     (newline)
     (print (args:usage opts))
     (print "Report bugs to ober at linbsd.org")))
 (exit 1))

(define (main)
    (args:parse (command-line-arguments) opts)
    (lmdb-close *db*))
(main)
