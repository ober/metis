;; fundamental metis ops
(defun ssdb/init ()
  (ssdb:connect))

(defun ssdb/close ()
  (ssdb:flushdb)
  (ssdb:disconnect))

(defun ssdb/have-we-seen-this-file (file)
  (ssdb/key? file))

(defun ssdb/mark-file-processed (file)
  (ssdb:set file "1"))

(defun ssdb/normalize-insert (db file)
  (let ((normalized (ssdb/normalize-file file)))
    (ssdb:insert db normalized)))


;; ported kunabi style ops

(defun ssdb/db-key? (key)
  (ssdb:exists key))

(defun ssdb/db-get (key)
  (ssdb:get key))

;; kv related
