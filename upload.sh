version=`cat setup.py | grep "version=" | cut -d "'" -f 2`
echo $version
twine upload dist/*-$version.*