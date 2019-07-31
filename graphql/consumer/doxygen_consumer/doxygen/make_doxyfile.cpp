#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
  if (argc < 3)
    {
      fprintf(stderr,
	      "Usage: make_doxyfile <skeleton file> <project_directory> [<output directory>]\n");

      exit(1);
    }

  FILE *skel = fopen(argv[1], "r");
  if (!skel)
    {
      fprintf(stderr, "Error opening %s:\n", argv[1]);
      perror(NULL);
      exit(1);
    }

  unsigned size = strlen(argv[2]);
  if (argc > 3)
    size += strlen(argv[3]);
  
  char buffer[1024+size];
  while (fgets(buffer, 1023, skel))
    {
      char *pct = index(buffer, '%');
      if (pct)
	{
	  if (pct[1] == 'I')
	    strcpy(pct, argv[2]);
	  else if (pct[1] == 'O')
	    {
	      if (argc > 3)
		strcpy(pct, argv[3]);
	      else
		strcpy(pct, "\n");
	    }
	}
      printf("%s", buffer);
    }

  fclose(skel);
}
