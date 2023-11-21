import base64

file = 'base64.txt'
image = open(file, 'rb')
image_read = image.read()
#image_64_encode = base64.encodebytes(image_read) #encodestring also works aswell as decodestring

image_64_decode = base64.decodebytes(image_read) 
image_result = open('base64.jpg', 'wb') # create a writable image and write the decoding result
#image_result.write(image_64_encode)
image_result.write(image_64_decode)
