
(defun lmdb-init ()
  )

(defun lmdb-close ()
  )

(defun lmdb-have-we-seen-this-file (file)
  (let ((db (lmdb-open (lmdb-get-db-path) lmdb-flags lmdb-mode)))
    (lmdb-get db file)))

(defun lmdb-mark-file-processed (file)
  (let ((db (lmdb-open (lmdb-get-db-path) lmdb-flags lmdb-mode)))
    (lmdb-put db file "1")))

(defun db-recreate-tables ()
  )

(defun lmdb-normalize-insert (db file)
  (lmdb-put db file "1"))

(defun lmdb-get-or-insert-id (table value)

  )

(defun lmdb-db-do-query (query)
