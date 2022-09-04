(in-package :metis)

(defvar *psql-files* (thread-safe-hash-table))
(defvar *metis-fields* (thread-safe-hash-table))
(defvar *metis-counters* (thread-safe-hash-table))
(defvar *metis-need-files* nil)

(defun psql-have-we-seen-this-file (file)
  (let* ((name (get-filename-hash file)))
    (multiple-value-bind (name seen) (gethash name *psql-files*)
    seen)))

(defun psql-init ()
  (let ((files (psql-do-query "select value from files")))
    (mapc
     (lambda (f)
       (format t "XXX f is ~a type:~a ~%" (car f) (type-of (car f)))
       (setf (gethash (car f) *psql-files*) t))
     files)
    (format t "XXX: size of *psql-files* ~a~%" (hash-table-size *psql-files*))))

(defun psql-mark-file-processed (file)
  (let ((name (get-filename-hash file)))
    (psql-do-query (format nil "insert into files(value) values('~a')" (or name file)))
    (setf (gethash name *psql-files*) t)))

(defun psql-do-query (query &optional db)
  (let ((database (or db "metis"))
        (user-name "metis")
        (password "metis")
        (host "localhost"))
    (block-if-syncing)
    (postmodern:with-connection
        `(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:query query))))

(defun psql-begin ()
  (if (equal *db-backend* :postgres)
      (let ((user-name "metis")
            (database "metis")
            (password "metis")
            (host "localhost"))
        (postmodern:with-connection
            `(,database ,user-name ,password ,host :pooled-p t)
          (postmodern:query "begin")))))

(defun psql-commit ()
  (let ((user-name "metis")
        (database "metis")
        (password "metis")
        (host "localhost"))
    (postmodern:with-connection
        `(,database ,user-name ,password ,host :pooled-p t)
      (postmodern:query "commit"))))

(defun psql-do-trans (query &optional db)
  (let ((database (or db "metis"))
        (user-name "metis")
        (password "metis")
        (host "localhost"))
    (block-if-syncing)
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

(defun psql-get-ids (record)
  (handler-case
      (let ((n 0)
            (fields *fields*)
            (record record))
        (loop for i in fields
              collect (let ((value (try-twice i (format nil "~A" (nth n record)))))
                        (incf n)
                        (if (null value)
                            (format t "i:~A val:~A try:~A~%"  i (nth n record) value))
                        value)))
    (t (e) (error-print "psql-get-ids" e))))

(defun psql-normalize-insert (record)
  (handler-case
      (let* ((record record)
             (values (psql-get-ids record))
             (tables (get-tables)))
        (pcall-queue:queue-push
         (format nil "~{~A~^	 ~}" values) to-db))
  (t (e) (error-print "psql-normalize-insert" e))))

(fare-memoization:define-memo-function psql-get-or-insert-id (table value)
  (setf *print-circle* nil)
  (let ((query (format nil "insert into ~A(value) select '~A' where not exists (select * from ~A where value = '~A')" table value table value)))
    ;;(format t "~%Q:~A~%" query)
    (psql-do-query query)
    (let ((id
            (flatten
             (car
              (car
               (psql-do-query
                (format nil "select id from ~A where value = '~A'" table value)))))))
      ;;(format t "gioip: table:~A value:~A id:~A~%" table value id)
      (if (listp id)
          (car id)
          id))))
