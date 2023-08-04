echo "Hi. Welcome aboard!"
rsync -avr --exclude '.git' --exclude 'data/*' --exclude '*.pyc*' /home/jovyan/socialnetwork_info_TFS/poc_neo4j /home/jovyan/udo_entry
echo "Done"

## Note of rsync
## https://linuxize.com/post/how-to-exclude-files-and-directories-with-rsync/#:~:text=With%20rsync%20you%20can%20also%20exclude%20files%20and,directories%20except%20those%20that%20match%20a%20certain%20pattern.
## muitlple exclude: 
## exclude all the files in folder_a, but folder_a is not excluded
## exclude all the files in folder_b and the folder_b
# rsync -avr --exclude 'folder_a/*' --exclude 'folder_b' /home/jovyan/z_aaa  /home/jovyan/socialnetwork_info_TFS 