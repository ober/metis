(in-package :metis)

;;(defparameter *q* (make-instance 'queue))
(defvar *h* (thread-safe-hash-table))
(defvar *db* nil)
(defvar *pcallers* 5)
(defvar *files* nil)
(defvar *conn* nil)
(defvar syncing nil)
(defvar *metis-need-hashes* nil)

(defparameter to-db (pcall-queue:make-queue))

(defvar *fields* '(
                   "additionalEventData"
                   "apiVersion"
                   "awsRegion"
                   "errorCode"
                   "errorMessage"
                   "eventCategory"
                   "eventID"
                   "eventName"
                   "eventSource"
                   "eventTime"
                   "eventType"
                   "eventVersion"
                   "managementEvent"
                   "readOnly"
                   "recipientAccountId"
                   "requestID"
                   "requestParameters"
                   "resources"
                   "responseElements"
                   "serviceEventDetails"
                   "sessionCredentialFromConsole"
                   "sharedEventID"
                   "sourceIPAddress"
                   "tlsDetails"
                   "userAgent"
                   "userIdentity"
                   "userName"
                   "vpcEndpointId"
                   ))

(defun db-init ()
  (cond
    ((equal :postgres *db-backend*) (psql/init))
    ((equal :manardb *db-backend*) (manardb/init))
    ((equal :lmdb *db-backend*) (lmdb/init))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-close ()
  (cond
    ((equal :postgres *db-backend*) (format t "fixme"))
    ((equal :manardb *db-backend*) (manardb:close-all-mmaps))
    ((equal :lmdb *db-backend*) (format t "fixme"))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-have-we-seen-this-file (file)
  (cond
    ((equal :postgres *db-backend*) (psql/have-we-seen-this-file file))
    ((equal :manardb *db-backend*) (manardb/have-we-seen-this-file file))
    ((equal :lmdb *db-backend*) (lmdb/have-we-seen-this-file file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-mark-file-processed (file)
  (cond
    ((equal :postgres *db-backend*) (psql/mark-file-processed file))
    ((equal :manardb *db-backend*) (manardb/mark-file-processed file))
    ((equal :lmdb *db-backend*) (lmdb/mark-file-processed file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-recreate-tables (db)
  (cond
    ((equal :postgres *db-backend*) (psql/recreate-tables))
    ((equal :manardb *db-backend*) (manardb/recreate-tables))
    ((equal :lmdb *db-backend*) (lmdb/recreate-tables))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun normalize-insert (record)
  (if record
      (handler-case
          (cond
            ((equal :postgres *db-backend*) (psql/normalize-insert record))
            ((equal :manardb *db-backend*) (manardb/normalize-insert record))
            ((equal :lmdb *db-backend*) (lmdb/normalize-insert record))
            (t (format t "unknown *db-backend*:~A~%" *db-backend*)))
        (t (e) (error-print "normalize-insert" e)))
      (format t "normalize-insert record empty")))

(defun db-get-or-insert-id (table value)
  (cond
    ((equal :postgres *db-backend*) (psql/get-or-insert-id table value))
    ((equal :manardb *db-backend*) (manardb/get-or-insert-id table value))
    ((equal :lmdb *db-backend*) (lmdb/get-or-insert-id table value))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-do-query (query)
  (cond
    ((equal :postgres *db-backend*)(psql/do-query query))
    ((equal :manardb *db-backend*)(manardb/do-query query))
    ((equal :lmdb *db-backend*)(lmdb/do-query query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-drop-table (query)
  (cond
    ((equal :postgres *db-backend*) (psql/drop-table query))
    ((equal :manardb *db-backend*) (manardb/drop-table query))
    ((equal :lmdb *db-backend*) (lmdb/drop-table query))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun make-safe-string (str)
  (if (stringp str)
      (replace-all str "'" "")
      str))

(defun process-record (record fields)
  (setf (gethash "userName" record) (get-user record))
  (loop for f in fields
        collect (format nil "~a" (gethash f record))))

(defun get-user (rec)
  "Given a record, find the username"
  (let ((ui (gethash "userIdentity" rec)))
    (when (hash-table-p ui)
      (let ((sc (gethash "sessionContext" ui)))
        (when (hash-table-p sc)
          (let ((si (gethash "sessionIssuer" sc)))
            (when (hash-table-p si)
              (let ((username (gethash "userName" si)))
                (when (stringp username)
                  username)))))))))

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

(defun block-if-syncing ()
  (loop while syncing
        do (progn
             (format t "s")
             (sleep 1))))
