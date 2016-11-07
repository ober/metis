(in-package :metis)


(defun manardb-recreate-tables ()
  (print "manardb-recreate-tables")
  )

(defun manardb-normalize-insert (record)
  (format t "manardb-nomalize-insert ~A~%" record)
  )

(defun manardb-get-or-insert-id (table value)
  (format t "manard-get-or-insert-id table:~A value:~A~%" table value)
  )

(defun manardb-drop-table (query)
  (format t "manardb-drop-table query:~A~%" query)
  )
