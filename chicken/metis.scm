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
(use srfi-69)
(use vector-lib)
(use z3)
(use natural-sort)
(use r7rs)

(define *db* (lmdb-open (make-pathname "/home/ubuntu/" "metis.mdb") mapsize: 100000000000))

;;(define *db* (lmdb-open (make-pathname "/Users/akkad/" "metis.mdb") key: (string->blob "omg") mapsize: 1000000000))

(define *fields* '(
		   "additionalEventData" ;; 0
		   "awsRegion" ;; 1
		   "errorCode" ;; 2
		   "errorMessage" ;; 3
		   "eventID" ;; 4
		   "eventName" ;; 5
		   "eventSource" ;; 6
		   "eventTime" ;; 7
		   "eventType" ;; 8
		   "eventVersion" ;; 9
		   "recipientAccountId" ;; 10
		   "requestID" ;; 11
		   "requestParameters" ;; 12
		   "resources" ;; 13
		   "responseElements" ;; 14
		   "sourceIPAddress" ;; 15
		   "userAgent" ;; 16
		   "userIdentity" ;; 17
		   "userName" ;; 18
		   ))


(define (add-event-type event)
  (format #t "hi"))

(define (parse-json-gz-file file)
  (let* ((gzip-stream (z3:open-compressed-input-file file))
	 (json (read-json gzip-stream)))
    (close-input-port gzip-stream)
    json))

(define (process-ct-file file)
  (when (or (file-is-new file) #t)
    (parse-ct-contents file)))

(define (parse-ct-contents file)
  (let* ((btime (current-milliseconds))
	 (json (parse-json-gz-file file))
	 (entries (vector->list (cdr (car json))))
	 (length (list-length entries)))
    (for-each
     (lambda (x)
       (normalize-insert (process-record x '() *fields*)))
     entries)
    (format #t "rps: ~A~%" (truncate (/ length (/ (- (current-milliseconds) btime) 1000)))))
  (mark-file-processed file))

(define (mark-file-processed file)
  (let ((files (with-input-from-string (blob->string (lmdb-ref *db* (string->blob "files"))) (cut deserialize))))
    (define files (cons file files))
    (lmdb-set! *db*
	       (string->blob "files")
	       (string->blob (with-output-to-string
			       (cut serialize files))))))

(define (init-files)
  (lmdb-begin *db*)
  (let ((files '()))
    (lmdb-set! *db*
	       (string->blob "files")
	       (string->blob (with-output-to-string
			       (cut serialize files)))))
  (lmdb-end *db*))


(define (file-is-new file)
  (let ((files (with-input-from-string
		   (blob->string
		    (lmdb-ref *db* (string->blob "files")))
		 (cut deserialize))))
    (if (hash-table? files)
	(member file files)
	#f)))

(define (list-files-done)
  (lmdb-begin *db*)
  (let ((files (with-input-from-string (blob->string (lmdb-ref *db* (string->blob "files"))) (cut deserialize))))
    (lmdb-end *db*)
    (for-each
     (lambda (x)
       (format #t "~A~%" x))
     files)))


(define (normalize-insert record)
  (let ((value-hash (make-hash-table)))
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
	  (hash-table-set! value-hash 'additionalEventData additionalEventData)
	  (hash-table-set! value-hash 'awsRegion awsRegion)
	  (hash-table-set! value-hash 'errorCode errorCode)
	  (hash-table-set! value-hash 'errorMessage errorMessage)
	  (hash-table-set! value-hash 'eventID eventID)
	  (hash-table-set! value-hash 'eventName eventName)
	  (hash-table-set! value-hash 'eventSource eventSource)
	  (hash-table-set! value-hash 'eventTime eventTime)
	  (hash-table-set! value-hash 'eventType eventType)
	  (hash-table-set! value-hash 'eventVersion eventVersion)
	  (hash-table-set! value-hash 'recipientAccountId recipientAccountId)
	  (hash-table-set! value-hash 'requestID requestID)
	  (hash-table-set! value-hash 'requestParameters requestParameters)
	  (hash-table-set! value-hash 'resources resources)
	  (hash-table-set! value-hash 'responseElements responseElements)
	  (hash-table-set! value-hash 'sourceIPAddress sourceIPAddress)
	  (hash-table-set! value-hash 'userAgent userAgent)
	  (hash-table-set! value-hash 'userIdentity userIdentity)
	  (hash-table-set! value-hash 'userName userName)

	  (let ((key (string-join (list eventTime eventName eventSource) "-"))
		(value (list additionalEventData awsRegion errorCode errorMessage eventID eventName eventSource eventType eventVersion recipientAccountId requestID requestParameters resources responseElements sourceIPAddress userAgent userIdentity userName)))
	    (lmdb-set! *db*
		       (string->blob (->string key))
		       (string->blob (with-output-to-string
				       (cut serialize value-hash))))
	    ))))

(define (process-record line results fields)
  (cond ((null-list? fields)
	 results)
	(else
	 (let ((value (get-value (car fields) line)))
	   (process-record line
			   (cons value results)
			   (cdr fields))))))


(define (print-records records)
  (for-each
   (lambda (x)
     (format-record x))
   (natural-sort records)))

(define (format-record record)
  (format #t "~A~%" record))


(define (ct-report-sync dir)
  (init-files)
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

(define (get-all-users)
  (lmdb-begin *db*)
  (let ((results '()))
    (for-each
     (lambda (key)
       (let* ((record (hash-table->alist (with-input-from-string
			  (blob->string (lmdb-ref *db* key))
			(cut deserialize))))
	      (userIdentity (cdr (assoc 'userIdentity record)))
	      (ver (cdr (assoc 'eventVersion record)))
	      (user (find-username record)))

	 (unless (member user results)
	   (set! results (cons user results)))))
	 (lmdb-keys *db*))
       (lmdb-end *db*)
       (print-records results)))

(define (find-username-102 ui record)
  (cond ((assoc 'userName ui) (cdr (assoc 'userName ui)))
	;; ((assoc 'sessionContext ui
	;; 	(cdr (assoc 'userName (
	;; 			       cdr (assoc 'sessionIssuer
	;; 					  (cdr (assoc 'sessionContext ui))))))))
	(else (format #t "102-bad: ~A~%" ui))))

(define (find-username-105 ui record)
  (cond ((assoc 'type ui) (cdr (assoc 'type ui)))
	((string= (cdr (assoc 'eventName record)) "BidFulfilledEvent") "BidFulfilledEvent")
	(else (format #t "105-bad: ~A~%" record))))

(define (find-username-104 ui record)
  (cond ((assoc 'type ui) (cdr (assoc 'type ui)))
	((string= (cdr (assoc 'eventName record)) "BidFulfilledEvent") "BidFulfilledEvent")
	(else (format #t "104-bad: ~A~%" record))))

(define (find-username record)
  (let ((ver (cdr (assoc 'eventVersion record)))
	(ui (cdr (assoc 'userIdentity record))))
    ;;(format #t "ver:~A userid:~A~%" ver userIdentity)
    (cond
     ((string= ver "1.02") (find-username-102 ui record))
     ((string= ver "1.04") (find-username-104 ui record))
     ((string= ver "1.05") (find-username-105 ui record))
     (else (format #f "Unsupported version ver:~A" ver)))))

(define (get-by-eventname eventname)
  (lmdb-begin *db*)
  (for-each
   (lambda (key)
     (let* ((ourkey (blob->string key))
	    (ourevent (list-ref (string-split ourkey "-") 3)))
       (if (string= ourevent eventname)
	   (format #t "~A~%" (hash-table->alist (with-input-from-string
				 (blob->string (lmdb-ref *db* key))
			       (cut deserialize)))))))
   (lmdb-keys *db*))
  (lmdb-end *db*))

(define (get-by-username username)
  (lmdb-begin *db*)
  (for-each
   (lambda (key)
     (let* ((ourkey (blob->string key))
	    (ourevent (list-ref (string-split ourkey "-") 3)))
       (if (string= ourevent username)
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
   (args:make-option (sn) (required: "NAME") "Return all records of NAME." (get-by-username arg))
   (args:make-option (ln) #:none "Return all unique users." (get-all-users))
   (args:make-option (lfd) #:none "Return all files processed." (list-files-done))
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
