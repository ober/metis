(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (thread-safe-hash-table))
(defvar *db* nil)
(defvar *pcallers* 5)
(defvar *files* nil)
(defvar *conn* nil)
(defvar syncing nil)

(defparameter to-db (pcall-queue:make-queue))

(defvar *fields* '(
                   :additionalEventData
                   :apiVersion
                   :awsRegion
                   :errorCode
                   :errorMessage
                   :eventCategory
                   :eventID
                   :eventName
                   :eventSource
                   :eventTime
                   :eventType
                   :eventVersion
                   :managementEvent
                   :readOnly
                   :recipientAccountId
                   :requestID
                   :requestParameters
                   :resources
                   :responseElements
                   :serviceEventDetails
                   :sessionCredentialFromConsole
                   :sharedEventID
                   :sourceIPAddress
                   :tlsDetails
                   :userAgent
                   :userIdentity
                   :userName
                   :vpcEndpointId
                   ))

(defun db-have-we-seen-this-file (file)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-have-we-seen-this-file file))
    ((equal :postgres *db-backend*) (sqlite-have-we-seen-this-file file))
    ((equal :manardb *db-backend*) (manardb-have-we-seen-this-file file))
    ((equal :lmdb *db-backend*) (lmdb-have-we-seen-this-file file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-mark-file-processed (file)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-mark-file-processed file))
    ((equal :postgres *db-backend*) (psql-mark-file-processed file))
    ((equal :manardb *db-backend*) (manardb-mark-file-processed file))
    ((equal :lmdb *db-backend*) (lmdb-mark-file-processed file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-recreate-tables (db)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-recreate-tables))
    ((equal :postgres *db-backend*) (psql-recreate-tables))
    ((equal :manardb *db-backend*) (manardb-recreate-tables))
    ((equal :lmdb *db-backend*) (lmdb-recreate-tables))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun normalize-insert (record)
  (when record
    (handler-case
        (cond
          ((equal :sqlite *db-backend*) (sqlite-normalize-insert record))
          ((equal :postgres *db-backend*) (psql-normalize-insert record))
          ((equal :manardb *db-backend*) (manardb-normalize-insert record))
          ((equal :lmdb *db-backend*) (lmdb-normalize-insert record))
          (t (format t "unknown *db-backend*:~A~%" *db-backend*)))
      (t (e) (error-print "normalize-insert" e))))
    )

(defun db-get-or-insert-id (table value)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-get-or-insert-id table value))
    ((equal :postgres *db-backend*) (psql-get-or-insert-id table value))
    ((equal :manardb *db-backend*) (manardb-get-or-insert-id table value))
    ((equal :lmdb *db-backend*) (lmdb-get-or-insert-id table value))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-do-query (query)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-do-query query))
    ((equal :postgres *db-backend*)(psql-do-query query))
    ((equal :manardb *db-backend*)(manardb-do-query query))
    ((equal :lmdb *db-backend*)(lmdb-do-query query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-drop-table (query)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-drop-table query))
    ((equal :postgres *db-backend*) (psql-drop-table query))
    ((equal :manardb *db-backend*) (manardb-drop-table query))
    ((equal :lmdb *db-backend*) (lmdb-drop-table query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun make-safe-string (str)
  (if (stringp str)
      (replace-all str "'" "")
      str))

(defun process-record (record fields)
  (handler-case
      (progn
  ;;      (dolist (r record)
;;          (if (and r
;;                   (keywordp r))
;;              (unless (member r fields :test (function string-equal))
;;                (format t "!! missing field. r: ~a type: ~a~%" r (type-of r)))))

        (loop for i in fields
              collect (make-safe-string (get-value i record))))
    (t (e) (error-print "process-record" e))))

(defun get-value (field record)
  (handler-case
      (let (record2 record)
        (cond
          ((equal :accessKeyId field) (fetch-value '(:|userIdentity| :|accessKeyId|) record2))
          ((equal :additionalEventData field) (getf record2 :|additionalEventData|))
          ((equal :apiVersion field) (getf record2 :|apiVersion|))
          ((equal :awsRegion field) (getf record2 :|awsRegion|))
          ((equal :errorCode field) (getf record2 :|errorCode|))
          ((equal :errorMessage field) (getf record2 :|errorMessage|))
          ((equal :eventCategory field) (getf record2 :|eventCategory|))
          ((equal :eventID field) (getf record2 :|eventID|))
          ((equal :eventName field) (getf record2 :|eventName|))
          ((equal :eventSource field) (getf record2 :|eventSource|))
          ((equal :eventTime field) (getf record2 :|eventTime|))
          ((equal :eventType field) (getf record2 :|eventType|))
          ((equal :eventVersion field) (getf record2 :|eventVersion|))
          ((equal :managementEvent field) (getf record2 :|managementEvent|))
          ((equal :readOnly field) (getf record2 :|readOnly|))
          ((equal :recipientAccountId field) (getf record2 :|recipientAccountId|))
          ((equal :requestID field) (getf record2 :|requestID|))
          ((equal :requestParameters field) (getf record2 :|requestParameters|))
          ((equal :resources field) (getf record2 :|resources|))
          ((equal :responseElements field) (getf record2 :|responseElements|))
          ((equal :serviceEventDetails field) (getf record2 :|serviceEventDetails|))
          ((equal :sessionCredentialFromConsole field) (getf record2 :|sessionCredentialFromConsole|))
          ((equal :sharedEventID field) (getf record2 :|sharedEventID|))
          ((equal :sourceIPAddress field) (get-hostname-by-ip (getf record2 :|sourceIPAddress|)))
          ((equal :tlsDetails field) (getf record2 :|tlsDetails|))
          ((equal :userAgent field) (getf record2 :|userAgent|))
          ((equal :userIdentity field) (getf record2 :|userIdentity|))
          ((equal :userName field) (fetch-value '(:|userIdentity| :|sessionContext| :|sessionIssuer| :|userName|) record2))
          ((equal :vpcEndpointId field) (getf record2 :|vpcEndpointId|))
          (t (format nil "Unknown arg:~A~%" field))))
        (t (e) (error-print (format "get-value field: ~a" field) e))))

(defun try-twice (table query)
  (let ((val (or
	      (ignore-errors (db-get-or-insert-id table query))
	      (db-get-or-insert-id table query))))
    val))

;;create unique index concurrently if not exists event_names_idx1 on event_names(id)

(defun get-index-value (table value)
  (let ((one (ignore-errors (db-get-or-insert-id table value))))
    (unless (typepc one 'integer)
      (setf one (db-get-or-insert-id table value)))
    one))

(defun get-tables()
  (format nil "~{~A~^, ~}" *fields*))

(defun load-file-values ()
  (unless *files*
    (setf *files*
	  (db-do-query "select value from files"))
    (mapcar #'(lambda (x)
		(setf (gethash (car x) *h*) t))
	    *files*))
  *h*)

(defun emit-drain-file (queue)
  "Dump the queue to a csv file for import to postgres"
  (format t "Draining ~A log entries into postgres...~%" (pcall-queue:queue-length queue))
  (with-open-file (drain "/tmp/loadme.txt" :direction :output :if-exists :supersede)
    (format drain "\COPY log(~{~A~^, ~}) FROM STDIN;~%" *fields*)
    (loop while (not (pcall-queue:queue-empty-p queue))
       do (progn
	    (format drain "~A~%" (pcall-queue:queue-pop queue))))
    (format drain "\\.~%"))
  (uiop:run-program (format nil "cat /tmp/loadme.txt|psql -U metis -d metis"))
  (format t "Draining complete.~%"))

(defun periodic-sync ()
  (if (null syncing)
      (progn
	(setf syncing t)
	;;(psql-commit)
	(let ((q-len (pcall-queue:queue-length to-db)))
	  (format t "Sync limit of ~A hit." q-len)
	  (emit-drain-file to-db)
	  ;;(psql-begin)
	  (setf syncing nil))
	(format t "sync already running...~%"))))

(defun block-if-syncing ()
  (loop while syncing
     do (progn
	  (format t "s")
	  (sleep 1))))
