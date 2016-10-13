(in-package :metis)
(defvar *db-backend* :sqlite)
;;(defvar *db-backend* :sqlite)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (make-hash-table :test 'equalp))
(defvar *db* nil)
(defvar *pcallers* 5)
(defvar *files* nil)
(defvar *sqlite-db* ":memory:")
;;(defvar *sqlite-db* "/tmp/metis.db")

(defvar *fields* '(
		   :additionalEventData
		   :awsRegion
		   :errorCode
		   :errorMessage
		   :eventID
		   :eventName
		   :eventSource
		   :eventTime
		   :eventType
		   :eventVersion
		   :recipientAccountId
		   :requestID
		   :requestParameters
		   :resources
		   :responseElements
		   :sourceIPAddress
		   :userAgent
		   :userIdentity
		   :userName
		   ))

(defun db-have-we-seen-this-file (x)
  (format t ".")
  (if (db-do-query (format nil "select id from files where value = '~A'" (file-namestring x)))
      t
      nil))

(defun db-mark-file-processed (x)
  (db-do-query
   (format nil "insert into files(value) values ('~A')" (file-namestring x)))
  (setf (gethash (file-namestring x) *h*) t))

(defun db-recreate-tables (query)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-recreate-tables))
    ((equal :postgres *db-backend*)(psql-recreate-tables))))


(defun db-get-or-insert-id (table value)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-get-or-insert-id table value))
    ((equal :postgres *db-backend*)(psql-get-or-insert-id table value))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-do-query (query)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-do-query query))
    ((equal :postgres *db-backend*)(psql-do-query query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun sqlite-drop-table (table &optional (db *sqlite-db*))
  (let* ((conn (sqlite:connect db)))
    (sqlite:execute-non-query conn (format nil "drop table if exists ~A" table))))

(defun sqlite-do-query (query &optional (db *sqlite-db*))
  (let* ((conn (sqlite:connect db)))
    (sqlite:execute-to-list conn query)))

(defun sqlite-recreate-tables (&optional (db *sqlite-db*))
  (let* ((conn (sqlite:connect db)))
    (sqlite-drop-table "files" db)
    (sqlite-drop-table "log" db)
    (sqlite:execute-non-query conn"drop view ct")
    (mapcar
     #'(lambda (x)
	 (sqlite-drop-table x db)) *fields*)
    (sqlite-create-tables)))

(defun sqlite-create-tables (&optional (db *sqlite-db*))
  (let* ((conn (sqlite:connect db)))
    (sqlite-create-table "files" db)
    (mapcar
     #'(lambda (x)
	 (sqlite-create-table x db)) *fields*)

    (format t "~%create table log(id integer primary key autoincrement, ~{~A ~^ integer, ~} integer)" *fields*)
    (sqlite:execute-non-query conn (format nil "create table log(id serial, ~{~A ~^ integer, ~} integer)" *fields*))
    (sqlite:execute-non-query conn
     (format nil "create view ct as select ~{~A.value as~:* ~A ~^,  ~} from log, ~{~A ~^, ~} where ~{~A.id = ~:*log.~A ~^and ~};" *fields* *fields* *fields*)
		   )))

(defun sqlite-create-table (table &optional (db *sqlite-db*))
  (let* ((conn (sqlite:connect db)))
    (format t "ct:~A db:~A~%" table db)
    (sqlite:execute-non-query conn (format nil "create table ~A(id serial, value text)" table))
    (sqlite:execute-non-query conn (format nil "create unique index ~A_idx1 on ~A(id)" table table))
    (sqlite:execute-non-query conn (format nil "create unique index ~A_idx2 on ~A(value)" table table))))

(defun db-drop-table (query)
  (cond
    ((equal :sqlite *db-backend*) (sqlite-drop-table query))
    ((equal :postgres *db-backend*)(psql-drop-table query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

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
  (psql-drop-table "files")
  (mapcar
   #'(lambda (x)
       (psql-drop-table x)) *fields*)
  (psql-do-query "drop table if exists log cascade")
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
	      (ignore-errors (db-get-or-insert-id table query))
	      (db-get-or-insert-id table query))))
    val))

;;create unique index concurrently if not exists event_names_idx1 on event_names(id)
(fare-memoization:define-memo-function psql-get-or-insert-id (table value)
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

(fare-memoization:define-memo-function sqlite-get-or-insert-id (table value)
  (setf *print-circle* nil)
  (let* ((database (or db "/tmp/metis.db"))
	 (conn (sqlite:connect database))
	 (query (format nil "insert into ~A(value) select '~A' where not exists (select * from ~A where value = '~A')" table value table value)))
    (sqlite:execute-single conn query)))

;; (let ((query
;;     ;;(format t "~%Q:~A~%" query)
;;     (psql-do-query query)
;;     (let ((id
;; 	   (flatten
;; 	    (car
;; 	     (car
;; 	      (psql-do-trans
;; 	       (format nil "select id from ~A where value = '~A'" table value)))))))
;;       ;;(format t "gioip: table:~A value:~A id:~A~%" table value id)
;;       (if (listp id)
;; 	  (car id)
;; 	  id))))

(defun get-index-value (table value)
  (let ((one (ignore-errors (db-get-id-or-insert-id table value))))
    (unless (typep one 'integer)
      (setf one (db-get-id-or-insert-id table value)))
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
