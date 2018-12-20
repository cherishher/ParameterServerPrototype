include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE})

# Unit Test
include_directories(${GTEST_INCLUDE})
add_executable(SimpleExample simple_example.cpp)
add_dependencies(SimpleExample gtest)
target_link_libraries(SimpleExample csci5570)
target_link_libraries(SimpleExample ${HUSKY_EXTERNAL_LIB})
target_link_libraries(SimpleExample ${GTEST_LIBRARIES})
target_link_libraries(SimpleExample ${GMOCK_LIBRARIES})
set_property(TARGET SimpleExample PROPERTY CXX_STANDARD 11)
add_dependencies(SimpleExample ${external_project_dependencies})


# SVM
include_directories(${GTEST_INCLUDE})
add_executable(SVM svm.cpp)
add_dependencies(SVM gtest)
target_link_libraries(SVM csci5570)
target_link_libraries(SVM ${HUSKY_EXTERNAL_LIB})
target_link_libraries(SVM ${GTEST_LIBRARIES})
target_link_libraries(SVM ${GMOCK_LIBRARIES})
set_property(TARGET SVM PROPERTY CXX_STANDARD 11)
add_dependencies(SVM ${external_project_dependencies})
