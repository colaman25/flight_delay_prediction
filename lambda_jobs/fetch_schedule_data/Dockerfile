FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY handler.py config.yaml ./

CMD ["handler.lambda_handler"]
