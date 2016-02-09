(defun psql-do-query (query)
  ;;(format t "db-hit:~A~%" query)
  (let ((database "metis")
	(user-name "metis")
	(password nil)
	(host "localhost"))
    (ignore-errors
      (postmodern:with-connection `(,database ,user-name ,password ,host :pooled-p t)
	(postmodern:query query)))))

(defun psql-drop-table (table)
  (ignore-errors
    (psql-do-query (format nil "drop table if exists ~A cascade" table))))

(defun psql-ensure-connection (db)
  (unless postmodern:*database*
    (setf postmodern:*database* (postmodern:connect db "metis" nil "localhost" :pooled-p t))))
