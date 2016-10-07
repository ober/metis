(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (make-hash-table :test 'equalp))
(defvar *DB* nil)
(defvar *pcallers* 5)
(defvar dbtype "postgres")
(defvar *files* nil)

(defvar *fields* '(
		   :recipientAccountId
		   :eventType
		   :eventID
		   :requestID
		   :responseElements
		   :requestParameters
		   :userAgent
		   :sourceIPAddress
		   :awsRegion
		   :eventName
		   :eventSource
		   :eventTime
		   :userIdentity
		   :eventVersion
		   :errorCode
		   :errorMessage
		   :additionalEventData
		   :resources
		   ))

(defun db-have-we-seen-this-file (x)
  (format t ".")
  (if (psql-do-query (format nil "select id from files where value = '~A'" (file-namestring x)))
      t
      nil))

(defun db-mark-file-processed (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x)))
  (setf (gethash (file-namestring x) *h*) t))

(defun db-mark-file-processed-preload (x)
  (psql-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x))))

(defun psql-do-query (query &optional db)
  (let ((database (or db "metis"))
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (postmodern:with-connection
	`(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:query query))))

(defun psql-do-trans (query &optional db)
  (let ((database (or db "metis"))
	(user-name "metis")
	(password "metis")
	(host "localhost"))
    (postmodern:with-connection
	`(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:with-transaction ()
	(postmodern:query query)))))

(defun psql-drop-table (table &optional db)
  (let ((database (or db "metis")))
    (format t "dt: ~A db:~A~%" table db)
    (psql-do-query (format nil "drop table if exists ~A cascade" table) database)))

(defun psql-ensure-connection (&optional db)
  (unless postmodern:*database*
    (setf postmodern:*database*
	  (postmodern:connect
	   (or db "metis")
	   "metis" "metis" "localhost" :pooled-p t))))

(defun process-record (record fields)
  (loop for i in fields
     collect (get-value i record)))



(defun get-value (field record)
  (cond
    ((equal :accessKeyId field)(fetch-value '(:|userIdentity| :|accessKeyId|) record))
    ((equal :additionalEventData field)(getf record :|additionalEventData|))
    ((equal :awsRegion field)(getf record :|awsRegion|))
    ((equal :errorCode field)(getf record :|errorCode|))
    ((equal :errorMessage field)(getf record :|errorMessage|))
    ((equal :eventID field)(getf record :|eventID|))
    ((equal :eventName field)(getf record :|eventName|))
    ((equal :eventSource field)(getf record :|eventSource|))
    ((equal :eventTime field)(getf record :|eventTime|))
    ((equal :eventType field)(getf record :|eventType|))
    ((equal :eventVersion field)(getf record :|eventVersion|))
    ((equal :recipientAccountId field)(getf record :|recipientAccountId|))
    ((equal :requestID field)(getf record :|requestID|))
    ((equal :requestParameters field)(getf record :|requestParameters|))
    ((equal :resources field)(getf record :|resources|))
    ((equal :responseElements field)(getf record :|responseElements|))
    ((equal :sourceIPAddress field)(getf record :|sourceIPAddress|))
    ((equal :userAgent field)(getf record :|userAgent|))
    ((equal :userIdentity field)(getf record :|userIdentity|))
    ((equal :userName field)(fetch-value '(:|userIdentity| :|sessionContext| :|sessionIssuer| :|userName|) record))
    (t (format nil "Unknown arg:~A~%" field))))

(defun psql-recreate-tables (&optional db)
  (psql-drop-table "files" database)
  (mapcar
   #'(lambda (x)
       (psql-drop-table x database)) *fields*)
  (psql-do-query "drop table if exists log cascade" database)
  (psql-create-tables))

(defun psql-create-tables (&optional db)
  (let ((database (or db "metis")))
    (psql-create-table "files" database)
    (mapcar
     #'(lambda (x)
	 (psql-create-table x database)) *fields*)

    (psql-do-query (format nil "create table if not exists log(id serial, ~{~A ~^ integer, ~} integer)" *fields*) database)
    (psql-do-query
     (format nil "create or replace view ct as select ~{~A.value as~:* ~A ~^,  ~} from log, ~{~A ~^, ~} where ~{~A.id = ~:*log.~A ~^and ~};" *fields* *fields* *fields*)
		   database)))

(defun psql-create-table (table &optional db)
  (let ((database (or db "metis")))
    (format t "ct:~A db:~A~%" table database)
    (psql-do-query (format nil "create table if not exists ~A(id serial, value text)" table) database)
    (psql-do-query (format nil "create unique index concurrently if not exists ~A_idx1 on ~A(id)" table table) database)
    (psql-do-query (format nil "create unique index concurrently if not exists ~A_idx2 on ~A(value)" table table) database)))

(defun try-twice (table query)
  (let ((val (or
	      (ignore-errors (get-id-or-insert-psql table query))
	      (get-id-or-insert-psql table query))))
    val))

;;create unique index concurrently if not exists event_names_idx1 on event_names(id)
(fare-memoization:define-memo-function get-id-or-insert-psql (table value)
  (setf *print-circle* nil)
  (let ((query (format nil "insert into ~A(value) select '~A' where not exists (select * from ~A where value = '~A')" table value table value)))
    ;;(format t "~%Q:~A~%" query)
    (psql-do-query query)
    (let ((id
	   (flatten
	    (car
	     (car
	      (psql-do-trans
	       (format nil "select id from ~A where value = '~A'" table value)))))))
      ;;(format t "gioip: table:~A value:~A id:~A~%" table value id)
      (if (listp id)
	  (car id)
	  id))))

(defun get-index-value (table value)
  (let ((one (ignore-errors (get-id-or-insert-psql table value))))
    (unless (typep one 'integer)
      (setf one (get-id-or-insert-psql table value)))
    one))


(defun get-ids(record)
  ;;(format t "omg: ni: ~A~%" (length omg)))
  (let ((n 0))
    (loop for i in *fields*
       collect (let ((value (try-twice i (format nil "~A" (nth n record)))))
		 (incf n)
		 (if (null value)
		     (format t "i:~A val:~A try:~A~%"  i (type-of (nth n record)) value))
		 value))))

(defun get-tables()
  (format nil "~{~A~^, ~}" *fields*))

(defun normalize-insert (record)
  (let ((values (get-ids record))
	(tables (get-tables)))
    (psql-do-query (format nil "insert into log(~{~A~^, ~}) values(~{~A~^, ~})" *fields* values))))

(defun load-file-values ()
  (unless *files*
    (setf *files*
	  (psql-do-query "select value from files" *DB*))
    (mapcar #'(lambda (x)
		(setf (gethash (car x) *h*) t))
	    *files*))
  *h*)
