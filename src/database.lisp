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

;; primary db functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun db-init ()
  (cond
    ((equal :manardb *db-backend*) (manardb/init))
    ((equal :ssdb *db-backend*) (ssdb/init))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-index (field days-ago)
  (cond
    ((equal :manardb *db-backend*) (manardb/index field days-ago))
    ((equal :ssdb *db-backend*) (ssdb/index field days-ago))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-stats ()
  (cond
    ((equal :manardb *db-backend*) (manardb/get-stats))
    ((equal :ssdb *db-backend*) (ssdb/get-stats))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-close ()
  (cond
    ((equal :manardb *db-backend*) (manardb:close-all-mmaps))
    ((equal :ssdb *db-backend*) (ssdb/close))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-by-name (name)
  "Get all records of name"
  (cond
    ((equal :manardb *db-backend*) (manardb/ct-get-by-name name))
    ((equal :ssdb *db-backend*) (ssdb/get-by-index name))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-by-event (event)
  "Get all records of name"
  (cond
    ((equal :manardb *db-backend*) (manardb/ct-get-by-eventName event))
    ((equal :ssdb *db-backend*) (ssdb/get-by-index event))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-by-errorcode (ec)
  "Get all records of name"
  (cond
    ((equal :manardb *db-backend*) (manardb/ct-get-by-errorcode ec))
    ((equal :ssdb *db-backend*) (ssdb/get-by-index ec))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-have-we-seen-this-file (file)
  (cond
    ((equal :manardb *db-backend*) (manardb/have-we-seen-this-file file))
    ((equal :ssdb *db-backend*) (ssdb/have-we-seen-this-file file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-mark-file-processed (file)
  (cond
    ((equal :manardb *db-backend*) (manardb/mark-file-processed file))
    ((equal :ssdb *db-backend*) (ssdb/mark-file-processed file))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-unique (field)
  (cond
    ((equal :manardb *db-backend*) (manardb/get-unique field))
    ((equal :ssdb *db-backend*) (ssdb/get-unique field))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-unique-names ()
  (cond
    ((equal :manardb *db-backend*) (manardb/get-name-list))
    ((equal :ssdb *db-backend*) (ssdb/get-unique-names))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-unique-errorcode ()
  (cond
    ((equal :manardb *db-backend*) (manardb/get-errorcode-list))
    ((equal :ssdb *db-backend*) (ssdb/get-unique-errorcode))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun db-get-unique-events ()
  (cond
    ((equal :manardb *db-backend*) (manardb/get-event-list))
    ((equal :ssdb *db-backend*) (ssdb/get-unique-events))
    (t (format t "unknown *db-backend*:~A~%" *db-backend*))))

(defun normalize-insert (record)
  (if record
      (handler-case
          (cond
            ((equal :manardb *db-backend*) (manardb/normalize-insert record))
            ((equal :ssdb *db-backend*) (ssdb/normalize-insert record))
            (t (format t "unknown *db-backend*:~A~%" *db-backend*)))
        (t (e) (error-print "normalize-insert" e)))
      (format t "normalize-insert record empty")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defun make-safe-string (str)
  (if (stringp str)
      (replace-all str "'" "")
      str))

(defun process-record (record fields)
  (let ((username (get-user record)))
    (if (string= username "NIL")
        (setf (gethash "userName" record) (alexandria:hash-table-plist (gethash "userIdentity" record)))
        (setf (gethash "userName" record) username))
  (loop for f in fields
        collect (format-field (gethash f record)))))

(defun format-field (field)
  (cond
    ((stringp field) (format nil "~a" field))
    ((numberp field) (format nil "~a" field))
    ((hash-table-p field) (format nil "~a" (alexandria:hash-table-plist field)))
    ((arrayp field) (format nil "array: ~a" field))
    ((listp field) (format nil "~a" field))
    ((null field) "n/a")
    (t (format nil "~a" field))))

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
